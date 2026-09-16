"""Tests 1-5: Module 1 hardening (tie-breaking deduplication, event-id integrity, PK consistency, atomic checkpoints)."""

import json
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from src.cdc.deduplication import deduplicate_cdc_events
from src.cdc.validation import validate_cdc_dataframe
from src.schemas.cdc_schema import (
    CDC_SPARK_SCHEMA,
    CURRENT_SCHEMA_VERSION,
    OPERATION_UPDATE,
    generate_event_id,
)
from src.state.checkpoint import CheckpointManager, CheckpointRegressionError


def test_01_deterministic_full_row_duplicate_tie_breaker(spark: SparkSession):
    """Test 1: Identical event_id, timestamp, and sequence but differing payload is deterministically resolved."""
    ev_id = generate_event_id("subscriptions", "SUB-000001", 50, OPERATION_UPDATE)
    ts = "2026-02-01T10:00:00Z"
    ingested_ts = "2026-02-01T10:05:00Z"

    # Two rows with identical key/timestamp/seq but different payload tags
    payload_a = json.dumps(
        {
            "subscription_id": "SUB-000001",
            "account_id": "ACC-1",
            "plan": "PRO",
            "status": "ACTIVE",
            "variant": "A",
        }
    )
    payload_b = json.dumps(
        {
            "subscription_id": "SUB-000001",
            "account_id": "ACC-1",
            "plan": "PRO",
            "status": "ACTIVE",
            "variant": "B",
        }
    )

    row_a = (
        ev_id,
        "subscriptions",
        OPERATION_UPDATE,
        "SUB-000001",
        50,
        ts,
        ingested_ts,
        1,
        CURRENT_SCHEMA_VERSION,
        payload_a,
    )
    row_b = (
        ev_id,
        "subscriptions",
        OPERATION_UPDATE,
        "SUB-000001",
        50,
        ts,
        ingested_ts,
        1,
        CURRENT_SCHEMA_VERSION,
        payload_b,
    )

    # Inverted input arrival
    df1 = spark.createDataFrame([row_a, row_b], CDC_SPARK_SCHEMA)
    df2 = spark.createDataFrame([row_b, row_a], CDC_SPARK_SCHEMA)

    deduped1, dup1 = deduplicate_cdc_events(df1)
    deduped2, dup2 = deduplicate_cdc_events(df2)

    winner1 = deduped1.collect()[0]["payload"]
    winner2 = deduped2.collect()[0]["payload"]

    # Canonical event hash tie-breaker must pick the exact same winner regardless of arrival
    assert winner1 == winner2
    assert dup1.count() == 1
    assert dup2.count() == 1


def test_02_event_id_mismatch_quarantined(spark: SparkSession):
    """Test 2: Event where event_id does not match recomputed hash is routed to quarantine with EVENT_ID_MISMATCH."""
    corrupt_id = "0" * 64
    payload = json.dumps(
        {
            "subscription_id": "SUB-000001",
            "account_id": "ACC-1",
            "plan": "PRO",
            "status": "ACTIVE",
        }
    )
    row = (
        corrupt_id,
        "subscriptions",
        OPERATION_UPDATE,
        "SUB-000001",
        50,
        "2026-02-01T10:00:00Z",
        "2026-02-01T10:05:00Z",
        1,
        CURRENT_SCHEMA_VERSION,
        payload,
    )
    df = spark.createDataFrame([row], CDC_SPARK_SCHEMA)
    valid_df, quarantine_df = validate_cdc_dataframe(df)

    assert valid_df.count() == 0
    assert quarantine_df.count() == 1
    reason = quarantine_df.collect()[0]["rejection_reason"]
    assert "EVENT_ID_MISMATCH" in reason


def test_03_payload_pk_business_key_mismatch_rejected(spark: SparkSession):
    """Test 3: Event where payload PK differs from envelope business_key is rejected to quarantine."""
    env_key = "SUB-000100"
    payload_key = "SUB-000200"
    ev_id = generate_event_id("subscriptions", env_key, 60, OPERATION_UPDATE)

    payload = json.dumps(
        {
            "subscription_id": payload_key,
            "account_id": "ACC-1",
            "plan": "PRO",
            "status": "ACTIVE",
        }
    )
    row = (
        ev_id,
        "subscriptions",
        OPERATION_UPDATE,
        env_key,
        60,
        "2026-02-01T10:00:00Z",
        "2026-02-01T10:05:00Z",
        1,
        CURRENT_SCHEMA_VERSION,
        payload,
    )
    df = spark.createDataFrame([row], CDC_SPARK_SCHEMA)
    valid_df, quarantine_df = validate_cdc_dataframe(df)

    assert valid_df.count() == 0
    assert quarantine_df.count() == 1
    reason = quarantine_df.collect()[0]["rejection_reason"]
    assert "PAYLOAD_KEY_MISMATCH" in reason


def test_04_batch_checkpoint_atomic_commit(temp_test_dir: Path):
    """Test 4: commit_batch atomically commits all table high-water marks and completed batch in one file."""
    mgr = CheckpointManager(checkpoint_dir=temp_test_dir)

    updates = {
        "accounts": 100,
        "subscriptions": 200,
        "invoices": 300,
        "payments": 400,
    }
    state = mgr.commit_batch(batch_id=1, table_sequences=updates)

    assert state.tables["accounts"].highest_source_sequence == 100
    assert state.tables["subscriptions"].highest_source_sequence == 200
    assert state.tables["invoices"].highest_source_sequence == 300
    assert state.tables["payments"].highest_source_sequence == 400
    assert 1 in state.completed_batch_ids

    # Verify reload from disk
    reloaded = CheckpointManager(checkpoint_dir=temp_test_dir).load_global_state()
    assert reloaded.tables["accounts"].highest_source_sequence == 100
    assert reloaded.completed_batch_ids == [1]


def test_05_simulated_checkpoint_write_failure_produces_no_partial_commit(temp_test_dir: Path):
    """Test 5: If any table sequence regresses, commit_batch aborts completely with no partial commit."""
    mgr = CheckpointManager(checkpoint_dir=temp_test_dir)

    # Initial valid commit
    mgr.commit_batch(
        batch_id=1,
        table_sequences={"accounts": 100, "subscriptions": 200, "invoices": 300, "payments": 400},
    )

    # Second commit with accounts and subscriptions advancing, but invoices regressing (300 -> 250)
    with pytest.raises(CheckpointRegressionError):
        mgr.commit_batch(
            batch_id=2,
            table_sequences={
                "accounts": 150,
                "subscriptions": 250,
                "invoices": 250,  # Regressed!
                "payments": 450,
            },
        )

    # Verify no partial advance occurred; state remains exactly as batch 1
    current = mgr.load_global_state()
    assert current.tables["accounts"].highest_source_sequence == 100
    assert current.tables["subscriptions"].highest_source_sequence == 200
    assert current.tables["invoices"].highest_source_sequence == 300
    assert current.tables["payments"].highest_source_sequence == 400
    assert 2 not in current.completed_batch_ids
