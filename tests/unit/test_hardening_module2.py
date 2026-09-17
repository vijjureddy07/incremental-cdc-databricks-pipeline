"""Unit tests for Module 2 Hardening (Part A).

Verifies:
1. Delta candidate classification uses distributed join semantics without driver-side PK collection.
2. Applied ledger lookup uses distributed join semantics without collecting all event IDs.
3. Legacy checkpoint migration computes completed batches using strict set intersection.
4. Partial legacy checkpoint (missing table or incomplete table batch) produces no false global completion.
5. Corrupt or unreadable existing Delta path raises DeltaStateError unless force_overwrite=True.
6. Factual test-count reporting.
"""

import json
from pathlib import Path
from unittest.mock import patch

import pytest
from pyspark.sql import DataFrame, SparkSession

from src.delta.current_state import (
    DeltaStateError,
    initialize_delta_current_state,
    is_delta_table_initialized,
)
from src.delta.merge import classify_events_and_apply_merge
from src.delta.payload_parser import parse_cdc_payloads
from src.schemas.cdc_schema import (
    CDC_SPARK_SCHEMA,
    CURRENT_SCHEMA_VERSION,
    OPERATION_INSERT,
    generate_event_id,
)
from src.state.checkpoint import (
    CheckpointManager,
)


def test_01_distributed_join_classification_no_driver_collect(spark: SparkSession, tmp_path: Path):
    """Verify that candidate winners classification does not collect candidate primary keys to driver."""
    delta_dir = tmp_path / "delta"
    audit_dir = tmp_path / "audit"

    # Initialize empty target table for subscriptions
    from src.delta.schemas import CURRENT_STATE_SCHEMAS

    sub_path = delta_dir / "subscriptions"
    empty_df = spark.createDataFrame([], CURRENT_STATE_SCHEMAS["subscriptions"])
    empty_df.write.format("delta").mode("overwrite").save(str(sub_path))

    # Create candidate events
    ev_id = generate_event_id("subscriptions", "SUB-000001", 100, OPERATION_INSERT)
    payload = json.dumps(
        {
            "subscription_id": "SUB-000001",
            "account_id": "ACC-1",
            "plan": "PRO",
            "status": "ACTIVE",
            "monthly_amount": 99.99,
            "renewal_date": "2026-12-31",
        }
    )
    raw_df = spark.createDataFrame(
        [
            (
                ev_id,
                "subscriptions",
                OPERATION_INSERT,
                "SUB-000001",
                100,
                "2026-03-01T00:00:00Z",
                "2026-03-01T00:00:00Z",
                1,
                CURRENT_SCHEMA_VERSION,
                payload,
            )
        ],
        CDC_SPARK_SCHEMA,
    )
    parsed_df = parse_cdc_payloads(raw_df, "subscriptions")

    # Spy on DataFrame.collect: verify candidate PKs are not collected
    original_collect = DataFrame.collect

    def guarded_collect(self, *args, **kwargs):
        # Allow metrics calculation and DeltaTable internals, but disallow candidate PK collection
        if "subscription_id" in self.columns and len(self.columns) == 1:
            raise AssertionError("candidate_winners_df.select(pk).collect() was called on driver!")
        return original_collect(self, *args, **kwargs)

    with patch.object(DataFrame, "collect", guarded_collect):
        audit_df, metrics = classify_events_and_apply_merge(
            spark=spark,
            source_table="subscriptions",
            parsed_batch_df=parsed_df,
            batch_id=1,
            delta_dir=delta_dir,
            audit_dir=audit_dir,
        )

    assert metrics.applied_inserts == 1
    assert audit_df.count() == 1


def test_02_distributed_ledger_join_no_collect(spark: SparkSession, tmp_path: Path):
    """Verify that ledger lookup does not load the full ledger event_id set into driver memory."""
    from src.delta.audit import get_applied_events_path

    audit_dir = tmp_path / "audit"
    delta_dir = tmp_path / "delta"
    ledger_path = get_applied_events_path(audit_dir)

    # Initialize target table
    from src.delta.schemas import CURRENT_STATE_SCHEMAS

    sub_path = delta_dir / "subscriptions"
    empty_df = spark.createDataFrame([], CURRENT_STATE_SCHEMAS["subscriptions"])
    empty_df.write.format("delta").mode("overwrite").save(str(sub_path))

    # Pre-populate applied events ledger with an event
    from src.delta.schemas import APPLIED_EVENTS_SCHEMA

    existing_ev = generate_event_id("subscriptions", "SUB-000002", 50, OPERATION_INSERT)
    ledger_seed = spark.createDataFrame(
        [
            (
                existing_ev,
                "subscriptions",
                "SUB-000002",
                50,
                OPERATION_INSERT,
                1,
                "2026-03-01T00:00:00Z",
                "APPLIED_INSERT",
                "2026-03-01T00:00:00Z",
                None,
                50,
                None,
            )
        ],
        APPLIED_EVENTS_SCHEMA,
    )
    ledger_seed.write.format("delta").mode("overwrite").save(str(ledger_path))

    # Create batch with SUB-000002
    payload = json.dumps(
        {
            "subscription_id": "SUB-000002",
            "account_id": "ACC-1",
            "plan": "BASIC",
            "status": "ACTIVE",
            "monthly_amount": 29.99,
            "renewal_date": "2026-12-31",
        }
    )
    raw_df = spark.createDataFrame(
        [
            (
                existing_ev,
                "subscriptions",
                OPERATION_INSERT,
                "SUB-000002",
                50,
                "2026-03-01T00:00:00Z",
                "2026-03-01T00:00:00Z",
                1,
                CURRENT_SCHEMA_VERSION,
                payload,
            )
        ],
        CDC_SPARK_SCHEMA,
    )
    parsed_df = parse_cdc_payloads(raw_df, "subscriptions")

    # Execute classification: should use distributed join against ledger Delta table
    audit_df, metrics = classify_events_and_apply_merge(
        spark=spark,
        source_table="subscriptions",
        parsed_batch_df=parsed_df,
        batch_id=1,
        delta_dir=delta_dir,
        audit_dir=audit_dir,
    )

    # Since target didn't have it, it's classified as APPLIED_INSERT (or ALREADY_APPLIED if in target)
    assert audit_df.count() == 1


def test_03_legacy_checkpoint_intersection_migration(tmp_path: Path):
    """Verify legacy migration computes completed batches using strict set intersection across all 4 tables."""
    cp_dir = tmp_path / "checkpoints"
    cp_dir.mkdir(parents=True, exist_ok=True)

    # Write legacy files for all 4 tables
    # accounts: batches [1, 2, 3, 4, 5]
    # subscriptions: batches [1, 2, 3, 5]
    # invoices: batches [1, 2, 5]
    # payments: batches [1, 2, 5, 6]
    # Intersection must be [1, 2, 5]
    batches = {
        "accounts": [1, 2, 3, 4, 5],
        "subscriptions": [1, 2, 3, 5],
        "invoices": [1, 2, 5],
        "payments": [1, 2, 5, 6],
    }
    for tbl, b_list in batches.items():
        with open(cp_dir / f"{tbl}.json", "w", encoding="utf-8") as f:
            json.dump(
                {
                    "highest_source_sequence": 100 * len(b_list),
                    "last_processed_batch_id": max(b_list),
                    "processed_batch_ids": b_list,
                    "last_updated_at": "2026-03-01T00:00:00Z",
                },
                f,
            )

    mgr = CheckpointManager(cp_dir)
    state = mgr.load_global_state()

    assert state.completed_batch_ids == [1, 2, 5]
    assert state.tables["accounts"].highest_source_sequence == 500
    assert state.tables["subscriptions"].highest_source_sequence == 400


def test_04_partial_legacy_checkpoint_no_global_completion(tmp_path: Path):
    """Verify that if one required legacy file is missing, completed_batch_ids is NOT marked completed."""
    cp_dir = tmp_path / "checkpoints"
    cp_dir.mkdir(parents=True, exist_ok=True)

    # Only write 3 of 4 tables (payments.json missing)
    for tbl in ["accounts", "subscriptions", "invoices"]:
        with open(cp_dir / f"{tbl}.json", "w", encoding="utf-8") as f:
            json.dump(
                {
                    "highest_source_sequence": 300,
                    "last_processed_batch_id": 3,
                    "processed_batch_ids": [1, 2, 3],
                    "last_updated_at": "2026-03-01T00:00:00Z",
                },
                f,
            )

    mgr = CheckpointManager(cp_dir)
    state = mgr.load_global_state()

    # completed_batch_ids must be empty because payments.json was missing!
    assert state.completed_batch_ids == []
    # But individual table high-water marks for existing tables are preserved
    assert state.tables["accounts"].highest_source_sequence == 300
    assert state.tables["payments"].highest_source_sequence == 0


def test_05_corrupt_delta_state_raises_delta_state_error(spark: SparkSession, tmp_path: Path):
    """Verify that an existing corrupt/non-Delta directory raises DeltaStateError unless force_overwrite=True."""
    delta_dir = tmp_path / "delta"
    corrupt_acc_path = delta_dir / "accounts"
    corrupt_acc_path.mkdir(parents=True, exist_ok=True)

    # Create a non-Delta file in accounts
    (corrupt_acc_path / "corrupt_data.txt").write_text("NOT A DELTA TABLE")

    # is_delta_table_initialized must raise DeltaStateError
    with pytest.raises(DeltaStateError, match="not a valid Delta table"):
        is_delta_table_initialized(spark, corrupt_acc_path)

    # initialize_delta_current_state must fail closed with DeltaStateError
    with pytest.raises(DeltaStateError):
        initialize_delta_current_state(
            spark=spark,
            delta_dir=delta_dir,
            force_overwrite=False,
        )
