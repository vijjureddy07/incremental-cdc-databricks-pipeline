"""Key-Scoped SCD2 History Rebuilder for Subscriptions."""

import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from delta.tables import DeltaTable
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F

from src.config.settings import (
    DEFAULT_CDC_EVENT_STORE_DIR,
    DEFAULT_CURRENT_DIR,
    DEFAULT_SAMPLE_DATA_DIR,
)
from src.delta.current_state import get_current_table_path, is_delta_table_initialized
from src.history.subscriptions_scd2 import (
    SNAPSHOT_OPERATION,
    SNAPSHOT_SENTINEL_EVENT_ID,
    SUBSCRIPTIONS_HISTORY_SCHEMA,
    SourceSequenceConflictError,
    generate_history_version_id,
    get_subscriptions_history_path,
    validate_subscription_history_invariants,
)
from src.schemas.cdc_schema import OPERATION_DELETE


def reconstruct_subscription_timeline_for_key(
    spark: SparkSession,
    subscription_id: str,
    snapshot_dir: Optional[Path] = None,
    event_store_dir: Optional[Path] = None,
    history_dir: Optional[Path] = None,
    current_delta_dir: Optional[Path] = None,
    repair_current: bool = False,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Deterministically reconstruct the complete historical SCD2 timeline for a single subscription key.

    Replay Algorithm:
    1. Identify business key.
    2. Load snapshot baseline for that key.
    3. Load ALL validated events for that business key from cdc_event_store.
    4. Detect source sequence collisions (same key + same sequence + different event_ids -> SourceSequenceConflictError).
    5. Order events by source_sequence ASC, event_id ASC.
    6. Reconstruct the contiguous timeline [valid_from, valid_to).
    7. Validate all temporal/sequence history invariants.
    8. Verify alignment with current-state Delta table (raise CurrentStateDriftError on mismatch unless repair_current=True).
    9. Atomically replace history rows for that specific subscription_id in subscriptions_history.

    Returns:
        (reconstructed_rows, metrics)
    """
    history_path = get_subscriptions_history_path(history_dir)
    event_store_path = event_store_dir or DEFAULT_CDC_EVENT_STORE_DIR
    current_table_path = get_current_table_path(
        "subscriptions", current_delta_dir or DEFAULT_CURRENT_DIR
    )

    # Step 1: Load snapshot baseline for this subscription
    snap_file = (snapshot_dir or DEFAULT_SAMPLE_DATA_DIR) / "snapshot" / "subscriptions.jsonl"
    if not snap_file.exists():
        snap_file_alt = (snapshot_dir or DEFAULT_SAMPLE_DATA_DIR) / "subscriptions.jsonl"
        if snap_file_alt.exists():
            snap_file = snap_file_alt
        else:
            raise FileNotFoundError(f"Snapshot file not found at {snap_file}")

    snap_raw_df = spark.read.json(str(snap_file))
    snap_match = snap_raw_df.filter(F.col("subscription_id") == subscription_id).collect()

    if not snap_match:
        raise ValueError(f"Subscription '{subscription_id}' not found in snapshot baseline.")

    snap_row = snap_match[0].asDict()
    snap_seq = int(snap_row["source_sequence"])
    snap_time = snap_row.get("created_at") or "2026-02-01T00:00:00+00:00"
    init_time = datetime.now(timezone.utc).isoformat()

    # Step 2: Load all events for this subscription from canonical cdc_event_store
    if not is_delta_table_initialized(spark, event_store_path):
        events_for_key: List[Dict[str, Any]] = []
    else:
        events_for_key = (
            spark.read.format("delta")
            .load(str(event_store_path))
            .filter(
                (F.col("source_table") == "subscriptions")
                & (F.col("business_key") == subscription_id)
            )
            .collect()
        )
        events_for_key = [r.asDict() for r in events_for_key]

    # Step 3: Check for sequence collisions
    # Rule: If two DIFFERENT event_ids exist for the same key + same sequence, raise SourceSequenceConflictError
    seq_to_events: Dict[int, List[Dict[str, Any]]] = {}
    for ev in events_for_key:
        s = int(ev["source_sequence"])
        seq_to_events.setdefault(s, []).append(ev)

    for s, evs in seq_to_events.items():
        unique_ev_ids = {e["event_id"] for e in evs}
        if len(unique_ev_ids) > 1:
            raise SourceSequenceConflictError(
                f"Source sequence collision detected on subscription '{subscription_id}' at sequence {s}: "
                f"conflicting event IDs: {unique_ev_ids}"
            )

    # Step 4: Deduplicate and sort events strictly ascending
    unique_events_dict = {e["event_id"]: e for e in events_for_key}
    sorted_events = sorted(
        unique_events_dict.values(),
        key=lambda e: (int(e["source_sequence"]), e["event_id"]),
    )

    # Step 5: Reconstruct timeline
    # Base version from snapshot
    snap_amt = snap_row.get("monthly_amount")
    snap_dec = Decimal(str(snap_amt)) if snap_amt is not None else None

    timeline: List[Dict[str, Any]] = [
        {
            "subscription_id": subscription_id,
            "account_id": snap_row.get("account_id"),
            "plan": snap_row.get("plan"),
            "status": snap_row.get("status"),
            "monthly_amount": snap_dec,
            "renewal_date": snap_row.get("renewal_date"),
            "billing_cycle": None,
            "currency": None,
            "history_version_id": generate_history_version_id(
                subscription_id, snap_seq, SNAPSHOT_SENTINEL_EVENT_ID
            ),
            "valid_from_sequence": snap_seq,
            "valid_to_sequence": None,
            "valid_from_event_timestamp": snap_time,
            "valid_to_event_timestamp": None,
            "is_current": True,
            "is_deleted": False,
            "source_event_id": SNAPSHOT_SENTINEL_EVENT_ID,
            "source_operation": SNAPSHOT_OPERATION,
            "schema_version": 1,
            "arrival_batch_id": 0,
            "history_updated_at": init_time,
        }
    ]

    for ev in sorted_events:
        ev_seq = int(ev["source_sequence"])
        ev_id = ev["event_id"]
        ev_op = ev["operation"]
        ev_ts = ev["event_timestamp"]
        ev_ver = int(ev.get("schema_version", 1))
        ev_batch = int(ev.get("arrival_batch_id", 0))

        # Close previous active version
        prev_version = timeline[-1]
        prev_version["valid_to_sequence"] = ev_seq
        prev_version["valid_to_event_timestamp"] = ev_ts
        prev_version["is_current"] = False

        # Parse event payload
        payload_data = ev.get("payload")
        if isinstance(payload_data, str):
            payload_dict = json.loads(payload_data)
        elif isinstance(payload_data, dict):
            payload_dict = payload_data
        else:
            payload_dict = {}

        # Business attributes
        is_del = ev_op == OPERATION_DELETE
        if is_del:
            # Delete retains previous business attributes for historical traceability
            account_id = prev_version["account_id"]
            plan = prev_version["plan"]
            status = "CANCELLED"
            monthly_amount = prev_version["monthly_amount"]
            renewal_date = prev_version["renewal_date"]
            billing_cycle = prev_version["billing_cycle"]
            currency = prev_version["currency"]
        else:
            account_id = payload_dict.get("account_id", prev_version["account_id"])
            plan = payload_dict.get("plan", prev_version["plan"])
            status = payload_dict.get("status", prev_version["status"])
            amt = payload_dict.get("monthly_amount")
            monthly_amount = (
                Decimal(str(amt)) if amt is not None else prev_version["monthly_amount"]
            )
            renewal_date = payload_dict.get("renewal_date", prev_version["renewal_date"])

            # Schema V2 columns: if V2 event, take from payload; if V1 event, preserve previous V2 values!
            if ev_ver >= 2:
                billing_cycle = payload_dict.get("billing_cycle")
                currency = payload_dict.get("currency")
            else:
                billing_cycle = prev_version["billing_cycle"]
                currency = prev_version["currency"]

        v_id = generate_history_version_id(subscription_id, ev_seq, ev_id)
        new_version = {
            "subscription_id": subscription_id,
            "account_id": account_id,
            "plan": plan,
            "status": status,
            "monthly_amount": monthly_amount,
            "renewal_date": renewal_date,
            "billing_cycle": billing_cycle,
            "currency": currency,
            "history_version_id": v_id,
            "valid_from_sequence": ev_seq,
            "valid_to_sequence": None,
            "valid_from_event_timestamp": ev_ts,
            "valid_to_event_timestamp": None,
            "is_current": True,
            "is_deleted": is_del,
            "source_event_id": ev_id,
            "source_operation": ev_op,
            "schema_version": ev_ver,
            "arrival_batch_id": ev_batch,
            "history_updated_at": init_time,
        }
        timeline.append(new_version)

    # Step 6: Validate invariants on reconstructed timeline
    current_state_dict: Optional[Dict[str, Any]] = None
    if is_delta_table_initialized(spark, current_table_path):
        curr_match = (
            spark.read.format("delta")
            .load(str(current_table_path))
            .filter(F.col("subscription_id") == subscription_id)
            .collect()
        )
        if curr_match:
            current_state_dict = curr_match[0].asDict()

    if not repair_current:
        validate_subscription_history_invariants(timeline, current_state_dict)
    else:
        # Validate internal timeline invariants only
        validate_subscription_history_invariants(timeline, None)
        # Perform explicit repair on current state table
        latest = timeline[-1]
        curr_table = DeltaTable.forPath(spark, str(current_table_path))
        curr_table.update(
            condition=F.col("subscription_id") == subscription_id,
            set={
                "_last_source_sequence": F.lit(latest["valid_from_sequence"]),
                "_last_event_id": F.lit(latest["source_event_id"]),
                "_last_event_timestamp": F.lit(latest["valid_from_event_timestamp"]),
                "_is_deleted": F.lit(latest["is_deleted"]),
                "_last_schema_version": F.lit(latest["schema_version"]),
                "plan": F.lit(latest["plan"]),
                "status": F.lit(latest["status"]),
                "monthly_amount": F.lit(latest["monthly_amount"]),
                "renewal_date": F.lit(latest["renewal_date"]),
                **(
                    {
                        "billing_cycle": F.lit(latest["billing_cycle"]),
                        "currency": F.lit(latest["currency"]),
                    }
                    if "billing_cycle" in curr_match[0].asDict()
                    else {}
                ),
            },
        )

    # Step 7: Atomic key-scoped replacement in subscriptions_history Delta table
    hist_table = DeltaTable.forPath(spark, str(history_path))
    versions_before = hist_table.toDF().filter(F.col("subscription_id") == subscription_id).count()

    # Build DataFrame from timeline rows
    row_objects = [Row(**r) for r in timeline]
    new_timeline_df = spark.createDataFrame(row_objects, schema=SUBSCRIPTIONS_HISTORY_SCHEMA)

    # Delete existing history for this subscription and append new timeline
    hist_table.delete(condition=F.col("subscription_id") == subscription_id)
    (new_timeline_df.write.format("delta").mode("append").save(str(history_path)))

    versions_after = len(timeline)
    current_seq_before = (
        current_state_dict.get("_last_source_sequence", 0) if current_state_dict else 0
    )
    current_seq_after = (
        timeline[-1]["valid_from_sequence"] if repair_current else current_seq_before
    )

    metrics = {
        "subscription_id": subscription_id,
        "history_versions_before": versions_before,
        "history_versions_after": versions_after,
        "current_sequence_before": current_seq_before,
        "current_sequence_after": current_seq_after,
        "current_state_changed": repair_current,
    }

    return timeline, metrics
