"""Operational SCD Type 2 history for subscriptions tracking source-system state evolution."""

import hashlib
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.config.settings import (
    DEFAULT_SAMPLE_DATA_DIR,
    DEFAULT_SUBSCRIPTIONS_HISTORY_DIR,
)
from src.delta.current_state import is_delta_table_initialized

SNAPSHOT_SENTINEL_EVENT_ID = "SNAPSHOT_INIT"
SNAPSHOT_OPERATION = "SNAPSHOT_INIT"


class HistoryInvariantError(ValueError):
    """Raised when temporal or sequence validity invariants of SCD2 history are violated."""

    pass


class CurrentStateDriftError(RuntimeError):
    """Raised when latest history row diverges from physical current-state table."""

    pass


class SourceSequenceConflictError(RuntimeError):
    """Raised when multiple distinct events share the exact same source sequence for a key."""

    pass


SUBSCRIPTIONS_HISTORY_SCHEMA = StructType(
    [
        StructField("subscription_id", StringType(), nullable=False),
        StructField("account_id", StringType(), nullable=True),
        StructField("plan", StringType(), nullable=True),
        StructField("status", StringType(), nullable=True),
        StructField("monthly_amount", DecimalType(12, 2), nullable=True),
        StructField("renewal_date", StringType(), nullable=True),
        StructField("billing_cycle", StringType(), nullable=True),
        StructField("currency", StringType(), nullable=True),
        StructField("history_version_id", StringType(), nullable=False),
        StructField("valid_from_sequence", LongType(), nullable=False),
        StructField("valid_to_sequence", LongType(), nullable=True),
        StructField("valid_from_event_timestamp", StringType(), nullable=False),
        StructField("valid_to_event_timestamp", StringType(), nullable=True),
        StructField("is_current", BooleanType(), nullable=False),
        StructField("is_deleted", BooleanType(), nullable=False),
        StructField("source_event_id", StringType(), nullable=False),
        StructField("source_operation", StringType(), nullable=False),
        StructField("schema_version", IntegerType(), nullable=False),
        StructField("arrival_batch_id", LongType(), nullable=False),
        StructField("history_updated_at", StringType(), nullable=False),
    ]
)


def generate_history_version_id(
    subscription_id: str,
    valid_from_sequence: int,
    source_event_id: str,
) -> str:
    """Generate a deterministic SHA-256 hash identifying a unique historical state version."""
    raw = f"{subscription_id}:{valid_from_sequence}:{source_event_id}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def get_subscriptions_history_path(history_dir: Optional[Path] = None) -> Path:
    """Return local path to the subscriptions_history Delta table."""
    return history_dir or DEFAULT_SUBSCRIPTIONS_HISTORY_DIR


def initialize_subscriptions_history_from_snapshot(
    spark: SparkSession,
    snapshot_dir: Optional[Path] = None,
    history_dir: Optional[Path] = None,
    force_overwrite: bool = False,
) -> int:
    """Seed subscriptions_history from the initial source snapshot.

    Each snapshot subscription becomes an initial historical version:
    - valid_from_sequence = snapshot.source_sequence
    - valid_to_sequence = NULL
    - is_current = True
    - is_deleted = False
    - source_event_id = 'SNAPSHOT_INIT'
    - source_operation = 'SNAPSHOT_INIT'
    - schema_version = 1
    - arrival_batch_id = 0
    - history_version_id = deterministic SHA256

    Returns:
        Row count written to subscriptions_history.
    """
    table_path = get_subscriptions_history_path(history_dir)
    table_path.parent.mkdir(parents=True, exist_ok=True)

    if not force_overwrite and is_delta_table_initialized(spark, table_path):
        return spark.read.format("delta").load(str(table_path)).count()

    snap_file = (snapshot_dir or DEFAULT_SAMPLE_DATA_DIR) / "snapshot" / "subscriptions.jsonl"
    if not snap_file.exists():
        # Fallback to direct snapshot_dir if subscriptions.jsonl is there
        snap_file_alt = (snapshot_dir or DEFAULT_SAMPLE_DATA_DIR) / "subscriptions.jsonl"
        if snap_file_alt.exists():
            snap_file = snap_file_alt
        else:
            raise FileNotFoundError(
                f"Cannot initialize subscriptions_history: snapshot file not found at {snap_file}."
            )

    init_time = datetime.now(timezone.utc).isoformat()

    # Read snapshot jsonl
    raw_df = spark.read.json(str(snap_file))

    # Build initial SCD2 DataFrame
    def map_snapshot_row(row):
        d = row.asDict()
        sub_id = d["subscription_id"]
        seq = int(d["source_sequence"])
        ev_ts = d.get("created_at") or init_time
        v_id = generate_history_version_id(sub_id, seq, SNAPSHOT_SENTINEL_EVENT_ID)

        amt = d.get("monthly_amount")
        dec_amt = Decimal(str(amt)) if amt is not None else None

        return Row(
            subscription_id=sub_id,
            account_id=d.get("account_id"),
            plan=d.get("plan"),
            status=d.get("status"),
            monthly_amount=dec_amt,
            renewal_date=d.get("renewal_date"),
            billing_cycle=None,
            currency=None,
            history_version_id=v_id,
            valid_from_sequence=seq,
            valid_to_sequence=None,
            valid_from_event_timestamp=ev_ts,
            valid_to_event_timestamp=None,
            is_current=True,
            is_deleted=False,
            source_event_id=SNAPSHOT_SENTINEL_EVENT_ID,
            source_operation=SNAPSHOT_OPERATION,
            schema_version=1,
            arrival_batch_id=0,
            history_updated_at=init_time,
        )

    rdd = raw_df.rdd.map(map_snapshot_row)
    history_df = spark.createDataFrame(rdd, schema=SUBSCRIPTIONS_HISTORY_SCHEMA)

    (
        history_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(table_path))
    )

    return history_df.count()


def validate_subscription_history_invariants(
    history_rows: List[Dict[str, Any]],
    current_state_row: Optional[Dict[str, Any]] = None,
) -> None:
    """Validate all 11 SCD2 history invariants for a given subscription timeline.

    Raises:
        HistoryInvariantError: If temporal, sequence, or structural invariants fail.
        CurrentStateDriftError: If latest historical state diverges from current state table.
    """
    if not history_rows:
        return

    # 1. history_version_id unique
    version_ids = [r["history_version_id"] for r in history_rows]
    if len(version_ids) != len(set(version_ids)):
        raise HistoryInvariantError(
            f"Invariant 1 failed: duplicate history_version_id in {[v for v in version_ids if version_ids.count(v) > 1]}"
        )

    # 2. valid_from_sequence unique
    from_seqs = [r["valid_from_sequence"] for r in history_rows]
    if len(from_seqs) != len(set(from_seqs)):
        raise HistoryInvariantError(
            f"Invariant 2 failed: duplicate valid_from_sequence in {[s for s in from_seqs if from_seqs.count(s) > 1]}"
        )

    # 3. Versions ordered strictly ascending by valid_from_sequence
    sorted_rows = sorted(history_rows, key=lambda r: r["valid_from_sequence"])
    if [r["valid_from_sequence"] for r in sorted_rows] != from_seqs:
        raise HistoryInvariantError(
            "Invariant 3 failed: history rows are not sorted ascending by valid_from_sequence"
        )

    # 4. Non-current rows must have valid_to_sequence > valid_from_sequence
    # 5. Adjacent rows: previous.valid_to_sequence == next.valid_from_sequence
    current_count = 0
    for idx, r in enumerate(sorted_rows):
        is_curr = r["is_current"]
        valid_from = r["valid_from_sequence"]
        valid_to = r["valid_to_sequence"]

        if is_curr:
            current_count += 1
            # 8. current row: valid_to_sequence is None
            if valid_to is not None:
                raise HistoryInvariantError(
                    f"Invariant 8 failed: current row has non-null valid_to_sequence={valid_to}"
                )
            # Current row must be the last sorted row
            if idx != len(sorted_rows) - 1:
                raise HistoryInvariantError(
                    f"Invariant 6/8 failed: is_current=True on non-latest version (index {idx} of {len(sorted_rows)})"
                )
        else:
            if valid_to is None:
                raise HistoryInvariantError(
                    f"Invariant 4 failed: non-current row at seq {valid_from} has NULL valid_to_sequence"
                )
            if valid_to <= valid_from:
                raise HistoryInvariantError(
                    f"Invariant 4 failed: valid_to_sequence ({valid_to}) <= valid_from_sequence ({valid_from})"
                )

        if idx > 0:
            prev_row = sorted_rows[idx - 1]
            prev_valid_to = prev_row["valid_to_sequence"]
            if prev_valid_to != valid_from:
                raise HistoryInvariantError(
                    f"Invariant 5 failed: gap or overlap between seq {prev_valid_to} and seq {valid_from}"
                )

    # 6 & 7. Exactly one current row for the key
    if current_count != 1:
        raise HistoryInvariantError(
            f"Invariant 6/7 failed: expected exactly 1 current version, got {current_count}"
        )

    # Current state alignment checks (9, 10, 11)
    if current_state_row is not None:
        latest_history = sorted_rows[-1]
        target_seq = current_state_row.get("_last_source_sequence")
        target_is_deleted = current_state_row.get("_is_deleted", False)

        # 9. highest history source sequence matches current-state _last_source_sequence
        if target_seq is not None and latest_history["valid_from_sequence"] != target_seq:
            raise CurrentStateDriftError(
                f"Invariant 9 failed: highest history seq ({latest_history['valid_from_sequence']}) "
                f"!= current state seq ({target_seq})"
            )

        # 10. current history is_deleted matches current-state _is_deleted
        if latest_history["is_deleted"] != target_is_deleted:
            raise CurrentStateDriftError(
                f"Invariant 10 failed: history is_deleted ({latest_history['is_deleted']}) "
                f"!= current state _is_deleted ({target_is_deleted})"
            )

        # 11. current business attributes match current-state attributes under corresponding schema
        if not target_is_deleted:
            business_fields = [
                "plan",
                "status",
                "monthly_amount",
                "renewal_date",
                "billing_cycle",
                "currency",
            ]
            for field in business_fields:
                if field in current_state_row and field in latest_history:
                    h_val = latest_history[field]
                    c_val = current_state_row[field]
                    # Compare decimals / strings carefully
                    if isinstance(h_val, Decimal) and c_val is not None:
                        c_val = Decimal(str(c_val))
                    if h_val != c_val:
                        raise CurrentStateDriftError(
                            f"Invariant 11 failed: field '{field}' mismatch between history ('{h_val}') "
                            f"and current state ('{c_val}')"
                        )
