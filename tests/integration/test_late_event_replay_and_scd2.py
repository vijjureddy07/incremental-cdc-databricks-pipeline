"""Comprehensive integration tests for SCD2 History, Late-Event Replay, Invariants, and Checkpoint Isolation."""

import json
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from src.delta.current_state import (
    initialize_delta_current_state,
    load_current_table,
)
from src.events.event_store import (
    initialize_event_store,
    upsert_to_event_store,
)
from src.generation.initial_state import generate_initial_state
from src.history.subscriptions_scd2 import (
    CurrentStateDriftError,
    HistoryInvariantError,
    SourceSequenceConflictError,
    generate_history_version_id,
    initialize_subscriptions_history_from_snapshot,
    validate_subscription_history_invariants,
)
from src.replay.audit import get_replay_audit_path
from src.replay.history_rebuilder import reconstruct_subscription_timeline_for_key
from src.replay.processor import process_pending_replays
from src.replay.queue import (
    STATUS_APPLIED,
    enqueue_late_events,
    initialize_late_event_queue,
)
from src.schemas.cdc_schema import (
    OPERATION_DELETE,
    OPERATION_UPDATE,
    generate_event_id,
)
from src.state.checkpoint import CheckpointManager


@pytest.fixture
def base_environment(spark: SparkSession, tmp_path: Path):
    """Set up snapshot baseline, current-state tables, event-store, and initial SCD2 history."""
    sample_dir = tmp_path / "sample_data"
    delta_dir = tmp_path / "delta"
    events_dir = delta_dir / "events" / "cdc_event_store"
    history_dir = delta_dir / "history" / "subscriptions_history"
    replay_dir = delta_dir / "replay"
    state_dir = tmp_path / "state"

    generate_initial_state(scale="tiny", seed=42, output_dir=sample_dir / "snapshot")
    initialize_delta_current_state(spark, snapshot_dir=sample_dir, delta_dir=delta_dir)
    initialize_event_store(spark, events_dir)
    initialize_late_event_queue(spark, replay_dir)
    initialize_subscriptions_history_from_snapshot(
        spark, snapshot_dir=sample_dir, history_dir=history_dir
    )

    return {
        "sample_dir": sample_dir,
        "delta_dir": delta_dir,
        "events_dir": events_dir,
        "history_dir": history_dir,
        "replay_dir": replay_dir,
        "state_dir": state_dir,
    }


def test_normal_scd2_initialization_and_invariants(spark: SparkSession, base_environment: dict):
    """Verify Requirements 13, 18, 19: Snapshot seeds SCD2, deterministic version IDs, 1 current row."""
    env = base_environment
    hist_df = spark.read.format("delta").load(str(env["history_dir"]))
    count = hist_df.count()
    assert count > 0

    # Verify deterministic version ID and exactly one current row per key
    sample_row = hist_df.first()
    expected_v_id = generate_history_version_id(
        sample_row["subscription_id"],
        sample_row["valid_from_sequence"],
        "SNAPSHOT_INIT",
    )
    assert sample_row["history_version_id"] == expected_v_id
    assert sample_row["is_current"] is True
    assert sample_row["valid_to_sequence"] is None

    # Check invariant validation passes on initial snapshot
    all_rows = [
        r.asDict()
        for r in hist_df.filter(f"subscription_id = '{sample_row['subscription_id']}'").collect()
    ]
    validate_subscription_history_invariants(all_rows, None)


def test_late_event_replay_flow_and_checkpoint_isolation(
    spark: SparkSession, base_environment: dict
):
    """Verify Requirements 20-30: Late event replay, interval repair, current-state unchanged, checkpoint untouched."""
    env = base_environment
    delta_dir = env["delta_dir"]
    events_dir = env["events_dir"]
    history_dir = env["history_dir"]
    replay_dir = env["replay_dir"]
    sample_dir = env["sample_dir"]
    state_dir = env["state_dir"]

    # Pick a subscription from snapshot
    curr_sub = load_current_table(spark, "subscriptions", delta_dir).first()
    sub_id = curr_sub["subscription_id"]
    snap_seq = curr_sub["_last_source_sequence"]

    # Normal forward updates: seq 120 (PRO), seq 150 (ENTERPRISE)
    seq_120 = snap_seq + 20
    seq_150 = snap_seq + 50
    ev_120_id = generate_event_id("subscriptions", sub_id, seq_120, OPERATION_UPDATE)
    ev_150_id = generate_event_id("subscriptions", sub_id, seq_150, OPERATION_UPDATE)

    ev_120 = {
        "event_id": ev_120_id,
        "source_table": "subscriptions",
        "business_key": sub_id,
        "source_sequence": seq_120,
        "operation": OPERATION_UPDATE,
        "event_timestamp": "2026-02-02T10:00:00+00:00",
        "ingested_timestamp": "2026-02-02T10:00:00+00:00",
        "batch_id": 1,
        "schema_version": 1,
        "payload": json.dumps(
            {"subscription_id": sub_id, "plan": "PRO", "status": "ACTIVE", "monthly_amount": 199.0}
        ),
        "is_late_on_arrival": False,
        "first_seen_at": "2026-02-02T10:00:00+00:00",
        "last_seen_at": "2026-02-02T10:00:00+00:00",
    }
    ev_150 = {
        "event_id": ev_150_id,
        "source_table": "subscriptions",
        "business_key": sub_id,
        "source_sequence": seq_150,
        "operation": OPERATION_UPDATE,
        "event_timestamp": "2026-02-02T11:00:00+00:00",
        "ingested_timestamp": "2026-02-02T11:00:00+00:00",
        "batch_id": 2,
        "schema_version": 1,
        "payload": json.dumps(
            {
                "subscription_id": sub_id,
                "plan": "ENTERPRISE",
                "status": "ACTIVE",
                "monthly_amount": 499.0,
            }
        ),
        "is_late_on_arrival": False,
        "first_seen_at": "2026-02-02T11:00:00+00:00",
        "last_seen_at": "2026-02-02T11:00:00+00:00",
    }
    upsert_to_event_store(spark, [ev_120, ev_150], events_dir)

    # Fast forward current state table to seq 150 (ENTERPRISE)
    from delta.tables import DeltaTable

    curr_table = DeltaTable.forPath(spark, str(delta_dir / "subscriptions"))
    curr_table.update(
        condition=f"subscription_id = '{sub_id}'",
        set={
            "_last_source_sequence": str(seq_150),
            "_last_event_id": f"'{ev_150_id}'",
            "plan": "'ENTERPRISE'",
            "status": "'ACTIVE'",
            "monthly_amount": "499.00",
        },
    )

    # Initialize forward checkpoint state
    ckpt_mgr = CheckpointManager(state_dir)
    ckpt_mgr.commit_batch(
        batch_id=2,
        table_sequences={
            "subscriptions": seq_150,
            "accounts": 100,
            "invoices": 100,
            "payments": 100,
        },
    )
    ckpt_before = ckpt_mgr.load_global_state()

    # Now a LATE UNSEEN EVENT arrives: seq 135 (status PAUSED, plan PRO)
    seq_135 = snap_seq + 35
    ev_135_id = generate_event_id("subscriptions", sub_id, seq_135, OPERATION_UPDATE)
    ev_135 = {
        "event_id": ev_135_id,
        "source_table": "subscriptions",
        "business_key": sub_id,
        "source_sequence": seq_135,
        "operation": OPERATION_UPDATE,
        "event_timestamp": "2026-02-02T10:30:00+00:00",
        "ingested_timestamp": "2026-02-02T12:00:00+00:00",
        "batch_id": 3,
        "schema_version": 1,
        "payload": json.dumps(
            {"subscription_id": sub_id, "plan": "PRO", "status": "PAUSED", "monthly_amount": 199.0}
        ),
        "is_late_on_arrival": True,
        "first_seen_at": "2026-02-02T12:00:00+00:00",
        "last_seen_at": "2026-02-02T12:00:00+00:00",
    }
    upsert_to_event_store(spark, [ev_135], events_dir)

    # Req 20: Enqueue into late event queue
    from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

    late_queue_schema = StructType(
        [
            StructField("event_id", StringType(), False),
            StructField("source_table", StringType(), False),
            StructField("business_key", StringType(), False),
            StructField("source_sequence", LongType(), False),
            StructField("batch_id", LongType(), False),
            StructField("schema_version", IntegerType(), False),
        ]
    )
    late_df = spark.createDataFrame(
        [(ev_135_id, "subscriptions", sub_id, seq_135, 3, 1)],
        schema=late_queue_schema,
    )
    enqueued = enqueue_late_events(spark, late_df, replay_dir)
    assert enqueued == 1

    # Req 21: Duplicate event enqueue does not create another entry
    enqueued_again = enqueue_late_events(spark, late_df, replay_dir)
    assert enqueued_again == 0

    # Execute late replay processor
    summary = process_pending_replays(
        spark=spark,
        replay_dir=replay_dir,
        event_store_dir=events_dir,
        history_dir=history_dir,
        current_delta_dir=delta_dir,
        snapshot_dir=sample_dir,
    )
    assert summary["applied"] == 1
    assert summary["conflict"] == 0
    assert summary["failed"] == 0

    # Req 22, 23, 24, 25: Verify corrected history timeline
    hist_rows = (
        spark.read.format("delta")
        .load(str(history_dir))
        .filter(f"subscription_id = '{sub_id}'")
        .orderBy("valid_from_sequence")
        .collect()
    )
    # Timeline should have 4 versions: snap_seq -> seq_120 -> seq_135 -> seq_150
    assert len(hist_rows) == 4
    _v0, v1, v2, v3 = hist_rows[0], hist_rows[1], hist_rows[2], hist_rows[3]

    # Req 23: Prior interval valid_to corrected
    assert v1["valid_from_sequence"] == seq_120
    assert v1["valid_to_sequence"] == seq_135  # Repaired from pointing to 150!
    assert v1["is_current"] is False

    # Req 24: Late version points to next sequence
    assert v2["valid_from_sequence"] == seq_135
    assert v2["valid_to_sequence"] == seq_150
    assert v2["status"] == "PAUSED"
    assert v2["is_current"] is False

    # Req 25: Later current history version remains current
    assert v3["valid_from_sequence"] == seq_150
    assert v3["valid_to_sequence"] is None
    assert v3["is_current"] is True
    assert v3["plan"] == "ENTERPRISE"

    # Req 26: Current-state row unchanged
    curr_after = (
        load_current_table(spark, "subscriptions", delta_dir)
        .filter(f"subscription_id = '{sub_id}'")
        .first()
    )
    assert curr_after["_last_source_sequence"] == seq_150
    assert curr_after["plan"] == "ENTERPRISE"

    # Req 27: CHECKPOINT ISOLATION - Global checkpoint state remains unchanged
    ckpt_after = ckpt_mgr.load_global_state()
    assert ckpt_after.completed_batch_ids == ckpt_before.completed_batch_ids
    assert (
        ckpt_after.tables["subscriptions"].highest_source_sequence
        == ckpt_before.tables["subscriptions"].highest_source_sequence
    )

    # Req 28: Replay audit persisted
    audit_rows = (
        spark.read.format("delta")
        .load(str(get_replay_audit_path(replay_dir)))
        .filter(f"event_id = '{ev_135_id}'")
        .collect()
    )
    assert len(audit_rows) == 1
    assert audit_rows[0]["status"] == STATUS_APPLIED

    # Req 29: Replay rerun is idempotent
    summary2 = process_pending_replays(
        spark=spark,
        replay_dir=replay_dir,
        event_store_dir=events_dir,
        history_dir=history_dir,
        current_delta_dir=delta_dir,
        snapshot_dir=sample_dir,
    )
    assert summary2["total_pending"] == 0  # No pending items!

    # Re-running key reconstruct directly does not duplicate rows
    _, metrics = reconstruct_subscription_timeline_for_key(
        spark=spark,
        subscription_id=sub_id,
        snapshot_dir=sample_dir,
        event_store_dir=events_dir,
        history_dir=history_dir,
        current_delta_dir=delta_dir,
    )
    assert metrics["history_versions_after"] == 4


def test_delete_replay_and_reactivation(spark: SparkSession, base_environment: dict):
    """Verify Requirements 31-34: Late historical DELETE creates tombstone interval, later reinsert remains current."""
    env = base_environment
    delta_dir = env["delta_dir"]
    events_dir = env["events_dir"]
    history_dir = env["history_dir"]
    sample_dir = env["sample_dir"]

    curr_sub = load_current_table(spark, "subscriptions", delta_dir).first()
    sub_id = curr_sub["subscription_id"]
    snap_seq = curr_sub["_last_source_sequence"]

    _seq_100 = snap_seq
    seq_140 = snap_seq + 40
    seq_170 = snap_seq + 70  # Late DELETE
    seq_200 = snap_seq + 100  # Reactivated PRO

    ev_140_id = generate_event_id("subscriptions", sub_id, seq_140, OPERATION_UPDATE)
    ev_170_id = generate_event_id("subscriptions", sub_id, seq_170, OPERATION_DELETE)
    ev_200_id = generate_event_id("subscriptions", sub_id, seq_200, OPERATION_UPDATE)

    ev_140 = {
        "event_id": ev_140_id,
        "source_table": "subscriptions",
        "business_key": sub_id,
        "source_sequence": seq_140,
        "operation": OPERATION_UPDATE,
        "event_timestamp": "2026-02-02T10:00:00+00:00",
        "ingested_timestamp": "2026-02-02T10:00:00+00:00",
        "batch_id": 1,
        "schema_version": 1,
        "payload": json.dumps({"subscription_id": sub_id, "plan": "PRO", "status": "ACTIVE"}),
        "is_late_on_arrival": False,
        "first_seen_at": "2026-02-02T10:00:00+00:00",
        "last_seen_at": "2026-02-02T10:00:00+00:00",
    }
    ev_170 = {
        "event_id": ev_170_id,
        "source_table": "subscriptions",
        "business_key": sub_id,
        "source_sequence": seq_170,
        "operation": OPERATION_DELETE,
        "event_timestamp": "2026-02-02T11:00:00+00:00",
        "ingested_timestamp": "2026-02-02T13:00:00+00:00",
        "batch_id": 3,
        "schema_version": 1,
        "payload": json.dumps({"subscription_id": sub_id, "cancellation_reason": "Churn"}),
        "is_late_on_arrival": True,
        "first_seen_at": "2026-02-02T13:00:00+00:00",
        "last_seen_at": "2026-02-02T13:00:00+00:00",
    }
    ev_200 = {
        "event_id": ev_200_id,
        "source_table": "subscriptions",
        "business_key": sub_id,
        "source_sequence": seq_200,
        "operation": OPERATION_UPDATE,
        "event_timestamp": "2026-02-02T12:00:00+00:00",
        "ingested_timestamp": "2026-02-02T12:00:00+00:00",
        "batch_id": 2,
        "schema_version": 1,
        "payload": json.dumps(
            {"subscription_id": sub_id, "plan": "ENTERPRISE", "status": "ACTIVE"}
        ),
        "is_late_on_arrival": False,
        "first_seen_at": "2026-02-02T12:00:00+00:00",
        "last_seen_at": "2026-02-02T12:00:00+00:00",
    }
    upsert_to_event_store(spark, [ev_140, ev_170, ev_200], events_dir)

    # Set current state to seq 200
    from delta.tables import DeltaTable

    curr_table = DeltaTable.forPath(spark, str(delta_dir / "subscriptions"))
    curr_table.update(
        condition=f"subscription_id = '{sub_id}'",
        set={
            "_last_source_sequence": str(seq_200),
            "_last_event_id": f"'{ev_200_id}'",
            "_is_deleted": "false",
            "plan": "'ENTERPRISE'",
            "status": "'ACTIVE'",
        },
    )

    # Reconstruct history
    timeline, _ = reconstruct_subscription_timeline_for_key(
        spark=spark,
        subscription_id=sub_id,
        snapshot_dir=sample_dir,
        event_store_dir=events_dir,
        history_dir=history_dir,
        current_delta_dir=delta_dir,
    )

    # Req 31: Late historical DELETE creates tombstone interval
    del_version = [v for v in timeline if v["valid_from_sequence"] == seq_170][0]
    assert del_version["is_deleted"] is True
    assert del_version["valid_to_sequence"] == seq_200
    assert del_version["is_current"] is False

    # Req 32: Later reinsert remains current
    curr_version = timeline[-1]
    assert curr_version["valid_from_sequence"] == seq_200
    assert curr_version["is_deleted"] is False
    assert curr_version["is_current"] is True

    # Req 33 & 34: Current state active and intervals contiguous
    validate_subscription_history_invariants(timeline, None)


def test_sequence_collision_handling(spark: SparkSession, base_environment: dict):
    """Verify Requirements 35, 36, 37: Sequence collision raises error, marks CONFLICT, leaves history unchanged."""
    env = base_environment
    delta_dir = env["delta_dir"]
    events_dir = env["events_dir"]
    history_dir = env["history_dir"]
    replay_dir = env["replay_dir"]
    sample_dir = env["sample_dir"]

    curr_sub = load_current_table(spark, "subscriptions", delta_dir).first()
    sub_id = curr_sub["subscription_id"]
    collision_seq = curr_sub["_last_source_sequence"] + 10

    # Two DIFFERENT event IDs with the same key and same sequence!
    ev_a_id = generate_event_id("subscriptions", sub_id, collision_seq, OPERATION_UPDATE)
    # Synthesize different event ID for collision test
    ev_b_id = "f" * 64

    ev_a = {
        "event_id": ev_a_id,
        "source_table": "subscriptions",
        "business_key": sub_id,
        "source_sequence": collision_seq,
        "operation": OPERATION_UPDATE,
        "event_timestamp": "2026-02-02T10:00:00+00:00",
        "ingested_timestamp": "2026-02-02T10:00:00+00:00",
        "batch_id": 1,
        "schema_version": 1,
        "payload": json.dumps({"subscription_id": sub_id, "plan": "PRO", "status": "ACTIVE"}),
        "is_late_on_arrival": False,
        "first_seen_at": "2026-02-02T10:00:00+00:00",
        "last_seen_at": "2026-02-02T10:00:00+00:00",
    }
    ev_b = {
        "event_id": ev_b_id,
        "source_table": "subscriptions",
        "business_key": sub_id,
        "source_sequence": collision_seq,
        "operation": OPERATION_UPDATE,
        "event_timestamp": "2026-02-02T10:00:00+00:00",
        "ingested_timestamp": "2026-02-02T10:00:00+00:00",
        "batch_id": 1,
        "schema_version": 1,
        "payload": json.dumps(
            {"subscription_id": sub_id, "plan": "ENTERPRISE", "status": "ACTIVE"}
        ),
        "is_late_on_arrival": False,
        "first_seen_at": "2026-02-02T10:00:00+00:00",
        "last_seen_at": "2026-02-02T10:00:00+00:00",
    }
    upsert_to_event_store(spark, [ev_a, ev_b], events_dir)

    # History count before
    hist_count_before = (
        spark.read.format("delta")
        .load(str(history_dir))
        .filter(f"subscription_id = '{sub_id}'")
        .count()
    )

    # Req 35: Rebuilding raises SourceSequenceConflictError
    with pytest.raises(SourceSequenceConflictError):
        reconstruct_subscription_timeline_for_key(
            spark=spark,
            subscription_id=sub_id,
            snapshot_dir=sample_dir,
            event_store_dir=events_dir,
            history_dir=history_dir,
            current_delta_dir=delta_dir,
        )

    # Req 36: Replay queue marks CONFLICT
    from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

    late_queue_schema = StructType(
        [
            StructField("event_id", StringType(), False),
            StructField("source_table", StringType(), False),
            StructField("business_key", StringType(), False),
            StructField("source_sequence", LongType(), False),
            StructField("batch_id", LongType(), False),
            StructField("schema_version", IntegerType(), False),
        ]
    )
    late_df = spark.createDataFrame(
        [(ev_a_id, "subscriptions", sub_id, collision_seq, 1, 1)],
        schema=late_queue_schema,
    )
    enqueue_late_events(spark, late_df, replay_dir)

    summary = process_pending_replays(
        spark=spark,
        replay_dir=replay_dir,
        event_store_dir=events_dir,
        history_dir=history_dir,
        current_delta_dir=delta_dir,
        snapshot_dir=sample_dir,
    )
    assert summary["conflict"] == 1

    # Req 37: History unchanged
    hist_count_after = (
        spark.read.format("delta")
        .load(str(history_dir))
        .filter(f"subscription_id = '{sub_id}'")
        .count()
    )
    assert hist_count_after == hist_count_before


def test_history_invariants_and_drift_detection(spark: SparkSession, base_environment: dict):
    """Verify Requirements 49-54: Invariants reject overlaps, gaps, multiple currents, and drift against current state."""
    # 49. Overlapping intervals
    overlapping_rows = [
        {
            "history_version_id": "v1",
            "valid_from_sequence": 100,
            "valid_to_sequence": 130,
            "is_current": False,
        },
        {
            "history_version_id": "v2",
            "valid_from_sequence": 120,
            "valid_to_sequence": None,
            "is_current": True,
        },
    ]
    with pytest.raises(HistoryInvariantError):
        validate_subscription_history_invariants(overlapping_rows)

    # 50. Duplicate valid_from_sequence
    dup_from_rows = [
        {
            "history_version_id": "v1",
            "valid_from_sequence": 100,
            "valid_to_sequence": 120,
            "is_current": False,
        },
        {
            "history_version_id": "v2",
            "valid_from_sequence": 100,
            "valid_to_sequence": None,
            "is_current": True,
        },
    ]
    with pytest.raises(HistoryInvariantError):
        validate_subscription_history_invariants(dup_from_rows)

    # 51. Multiple current versions
    multi_curr_rows = [
        {
            "history_version_id": "v1",
            "valid_from_sequence": 100,
            "valid_to_sequence": None,
            "is_current": True,
        },
        {
            "history_version_id": "v2",
            "valid_from_sequence": 120,
            "valid_to_sequence": None,
            "is_current": True,
        },
    ]
    with pytest.raises(HistoryInvariantError):
        validate_subscription_history_invariants(multi_curr_rows)

    # 52. Missing current version
    no_curr_rows = [
        {
            "history_version_id": "v1",
            "valid_from_sequence": 100,
            "valid_to_sequence": 120,
            "is_current": False,
        },
        {
            "history_version_id": "v2",
            "valid_from_sequence": 120,
            "valid_to_sequence": 150,
            "is_current": False,
        },
    ]
    with pytest.raises(HistoryInvariantError):
        validate_subscription_history_invariants(no_curr_rows)

    # 53. History vs Current state sequence mismatch raises CurrentStateDriftError
    valid_timeline = [
        {
            "history_version_id": "v1",
            "valid_from_sequence": 100,
            "valid_to_sequence": 120,
            "is_current": False,
            "is_deleted": False,
            "plan": "BASIC",
            "status": "ACTIVE",
            "monthly_amount": Decimal("49.00"),
            "renewal_date": "2027-01-01",
            "billing_cycle": None,
            "currency": None,
        },
        {
            "history_version_id": "v2",
            "valid_from_sequence": 120,
            "valid_to_sequence": None,
            "is_current": True,
            "is_deleted": False,
            "plan": "PRO",
            "status": "ACTIVE",
            "monthly_amount": Decimal("199.00"),
            "renewal_date": "2027-01-01",
            "billing_cycle": None,
            "currency": None,
        },
    ]
    stale_current_state = {
        "_last_source_sequence": 100,  # History is at 120!
        "_is_deleted": False,
        "plan": "PRO",
    }
    with pytest.raises(CurrentStateDriftError):
        validate_subscription_history_invariants(valid_timeline, stale_current_state)

    # 54. Current business attribute mismatch raises CurrentStateDriftError
    drift_current_state = {
        "_last_source_sequence": 120,
        "_is_deleted": False,
        "plan": "ENTERPRISE",  # History has PRO!
    }
    with pytest.raises(CurrentStateDriftError):
        validate_subscription_history_invariants(valid_timeline, drift_current_state)


def test_late_v1_replay_across_v2_migration_and_crash_recovery(
    spark: SparkSession, base_environment: dict
):
    """Verify Requirements 30, 46, 47, 48: Crash recovery, late V1 replay after V2 migration, V2 columns preservation."""
    env = base_environment
    delta_dir = env["delta_dir"]
    events_dir = env["events_dir"]
    history_dir = env["history_dir"]
    replay_dir = env["replay_dir"]
    sample_dir = env["sample_dir"]

    from src.schema_evolution.migrations import migrate_subscriptions_to_v2

    # Step 1: Migrate current-state table to V2
    migrate_subscriptions_to_v2(spark, delta_dir)

    curr_sub = load_current_table(spark, "subscriptions", delta_dir).first()
    sub_id = curr_sub["subscription_id"]
    snap_seq = curr_sub["_last_source_sequence"]

    seq_200 = snap_seq + 100  # V1 event
    seq_210 = snap_seq + 110  # Late V1 event
    seq_220 = snap_seq + 120  # V2 event

    ev_200_id = generate_event_id("subscriptions", sub_id, seq_200, OPERATION_UPDATE)
    ev_210_id = generate_event_id("subscriptions", sub_id, seq_210, OPERATION_UPDATE)
    ev_220_id = generate_event_id("subscriptions", sub_id, seq_220, OPERATION_UPDATE)

    ev_200 = {
        "event_id": ev_200_id,
        "source_table": "subscriptions",
        "business_key": sub_id,
        "source_sequence": seq_200,
        "operation": OPERATION_UPDATE,
        "event_timestamp": "2026-02-02T10:00:00+00:00",
        "ingested_timestamp": "2026-02-02T10:00:00+00:00",
        "batch_id": 1,
        "schema_version": 1,
        "payload": json.dumps({"subscription_id": sub_id, "plan": "PRO", "status": "ACTIVE"}),
        "is_late_on_arrival": False,
        "first_seen_at": "2026-02-02T10:00:00+00:00",
        "last_seen_at": "2026-02-02T10:00:00+00:00",
    }
    # V2 forward event at seq 220
    ev_220 = {
        "event_id": ev_220_id,
        "source_table": "subscriptions",
        "business_key": sub_id,
        "source_sequence": seq_220,
        "operation": OPERATION_UPDATE,
        "event_timestamp": "2026-02-02T12:00:00+00:00",
        "ingested_timestamp": "2026-02-02T12:00:00+00:00",
        "batch_id": 4,
        "schema_version": 2,
        "payload": json.dumps(
            {
                "subscription_id": sub_id,
                "plan": "ENTERPRISE",
                "status": "ACTIVE",
                "billing_cycle": "ANNUAL",
                "currency": "USD",
            }
        ),
        "is_late_on_arrival": False,
        "first_seen_at": "2026-02-02T12:00:00+00:00",
        "last_seen_at": "2026-02-02T12:00:00+00:00",
    }
    upsert_to_event_store(spark, [ev_200, ev_220], events_dir)

    # Fast forward current state table to V2 seq 220
    from delta.tables import DeltaTable

    curr_table = DeltaTable.forPath(spark, str(delta_dir / "subscriptions"))
    curr_table.update(
        condition=f"subscription_id = '{sub_id}'",
        set={
            "_last_source_sequence": str(seq_220),
            "_last_event_id": f"'{ev_220_id}'",
            "_last_schema_version": "2",
            "plan": "'ENTERPRISE'",
            "status": "'ACTIVE'",
            "billing_cycle": "'ANNUAL'",
            "currency": "'USD'",
        },
    )

    # Now a late V1 event arrives: seq 210
    ev_210 = {
        "event_id": ev_210_id,
        "source_table": "subscriptions",
        "business_key": sub_id,
        "source_sequence": seq_210,
        "operation": OPERATION_UPDATE,
        "event_timestamp": "2026-02-02T11:00:00+00:00",
        "ingested_timestamp": "2026-02-02T13:00:00+00:00",
        "batch_id": 5,
        "schema_version": 1,
        "payload": json.dumps({"subscription_id": sub_id, "plan": "PRO", "status": "ACTIVE"}),
        "is_late_on_arrival": True,
        "first_seen_at": "2026-02-02T13:00:00+00:00",
        "last_seen_at": "2026-02-02T13:00:00+00:00",
    }
    upsert_to_event_store(spark, [ev_210], events_dir)

    # Enqueue late V1 event
    from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

    late_df = spark.createDataFrame(
        [(ev_210_id, "subscriptions", sub_id, seq_210, 5, 1)],
        schema=StructType(
            [
                StructField("event_id", StringType(), False),
                StructField("source_table", StringType(), False),
                StructField("business_key", StringType(), False),
                StructField("source_sequence", LongType(), False),
                StructField("batch_id", LongType(), False),
                StructField("schema_version", IntegerType(), False),
            ]
        ),
    )
    enqueue_late_events(spark, late_df, replay_dir)

    # Simulate Partial Crash (Req 30):
    # Reconstruct history first, but leave queue in PENDING
    reconstruct_subscription_timeline_for_key(
        spark=spark,
        subscription_id=sub_id,
        snapshot_dir=sample_dir,
        event_store_dir=events_dir,
        history_dir=history_dir,
        current_delta_dir=delta_dir,
    )
    hist_count_after_first = (
        spark.read.format("delta")
        .load(str(history_dir))
        .filter(f"subscription_id = '{sub_id}'")
        .count()
    )

    # Run replay processor (simulating restart after crash)
    summary = process_pending_replays(
        spark=spark,
        replay_dir=replay_dir,
        event_store_dir=events_dir,
        history_dir=history_dir,
        current_delta_dir=delta_dir,
        snapshot_dir=sample_dir,
    )
    assert summary["applied"] == 1

    # Req 30: History row count unchanged (no duplicate history rows created during crash retry)
    hist_count_after_retry = (
        spark.read.format("delta")
        .load(str(history_dir))
        .filter(f"subscription_id = '{sub_id}'")
        .count()
    )
    assert hist_count_after_retry == hist_count_after_first

    # Req 46, 47, 48:
    # 47: Late V1 replay did not erase V2 current fields
    curr_after = (
        load_current_table(spark, "subscriptions", delta_dir)
        .filter(f"subscription_id = '{sub_id}'")
        .first()
    )
    assert curr_after["billing_cycle"] == "ANNUAL"
    assert curr_after["currency"] == "USD"
    assert curr_after["_last_schema_version"] == 2

    # 48: History supports both V1 rows (NULL billing_cycle/currency) and V2 rows (populated)
    history_versions = (
        spark.read.format("delta")
        .load(str(history_dir))
        .filter(f"subscription_id = '{sub_id}'")
        .orderBy("valid_from_sequence")
        .collect()
    )
    # v0 (snap), v1 (seq 200, V1), v2 (seq 210, V1), v3 (seq 220, V2)
    assert len(history_versions) == 4
    v_late_v1 = [v for v in history_versions if v["valid_from_sequence"] == seq_210][0]
    assert v_late_v1["schema_version"] == 1
    assert v_late_v1["billing_cycle"] is None
    assert v_late_v1["currency"] is None

    v_v2 = [v for v in history_versions if v["valid_from_sequence"] == seq_220][0]
    assert v_v2["schema_version"] == 2
    assert v_v2["billing_cycle"] == "ANNUAL"
    assert v_v2["currency"] == "USD"
    assert v_v2["is_current"] is True
