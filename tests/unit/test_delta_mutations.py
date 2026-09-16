"""Tests 13-29: Delta MERGE mutations (INSERT, UPDATE, DELETE, within-batch collapsing, protective tombstones, re-inserts)."""

import json
from pathlib import Path

from pyspark.sql import SparkSession

from src.delta.audit import get_applied_events_path, upsert_applied_events
from src.delta.current_state import (
    initialize_delta_current_state,
    load_active_table,
    load_current_table,
)
from src.delta.merge import classify_events_and_apply_merge
from src.delta.payload_parser import parse_cdc_payloads
from src.generation.initial_state import generate_initial_state
from src.schemas.cdc_schema import (
    CDC_SPARK_SCHEMA,
    CURRENT_SCHEMA_VERSION,
    OPERATION_DELETE,
    OPERATION_INSERT,
    OPERATION_UPDATE,
    generate_event_id,
)


def _setup_test_env(base_dir: Path, spark: SparkSession):
    snap_dir = base_dir / "sample" / "snapshot"
    generate_initial_state(scale="tiny", seed=42, output_dir=snap_dir)
    delta_dir = base_dir / "delta" / "current"
    audit_dir = base_dir / "delta" / "audit"
    initialize_delta_current_state(
        spark=spark, snapshot_dir=base_dir / "sample", delta_dir=delta_dir
    )
    return delta_dir, audit_dir


def test_13_to_16_insert_handling(spark: SparkSession, temp_test_dir: Path):
    """Tests 13-16: INSERT creates new target row, updates sequence and event_id, and records in ledger."""
    delta_dir, audit_dir = _setup_test_env(temp_test_dir, spark)

    new_sub_id = "SUB-999999"
    ev_id = generate_event_id("subscriptions", new_sub_id, 950, OPERATION_INSERT)
    payload = json.dumps(
        {
            "subscription_id": new_sub_id,
            "account_id": "ACC-000001",
            "plan": "ENTERPRISE",
            "status": "ACTIVE",
            "monthly_amount": 499.00,
            "renewal_date": "2026-12-31",
        }
    )
    raw_row = (
        ev_id,
        "subscriptions",
        OPERATION_INSERT,
        new_sub_id,
        950,
        "2026-03-01T10:00:00Z",
        "2026-03-01T10:01:00Z",
        1,
        CURRENT_SCHEMA_VERSION,
        payload,
    )
    raw_df = spark.createDataFrame([raw_row], CDC_SPARK_SCHEMA)
    parsed_df = parse_cdc_payloads(raw_df, "subscriptions")

    audit_df, metrics = classify_events_and_apply_merge(
        spark=spark,
        source_table="subscriptions",
        parsed_batch_df=parsed_df,
        batch_id=1,
        delta_dir=delta_dir,
    )
    upsert_applied_events(spark, audit_df, audit_dir)

    # Test 13: Row created
    sub_df = load_current_table(spark, "subscriptions", delta_dir).filter(
        f"subscription_id = '{new_sub_id}'"
    )
    assert sub_df.count() == 1
    row = sub_df.collect()[0]

    # Test 14: Target sequence updated
    assert row["_last_source_sequence"] == 950

    # Test 15: Target event ID updated
    assert row["_last_event_id"] == ev_id
    assert row["_is_deleted"] is False
    assert row["plan"] == "ENTERPRISE"

    # Test 16: Applied ledger contains event
    ledger_df = (
        spark.read.format("delta")
        .load(str(get_applied_events_path(audit_dir)))
        .filter(f"event_id = '{ev_id}'")
    )
    assert ledger_df.count() == 1
    assert ledger_df.collect()[0]["apply_outcome"] == "APPLIED_INSERT"


def test_17_to_20_update_handling(spark: SparkSession, temp_test_dir: Path):
    """Tests 17-20: UPDATE modifies existing row, preserves PK, higher sequence wins, lower sequence cannot overwrite."""
    delta_dir, audit_dir = _setup_test_env(temp_test_dir, spark)
    target_sub = "SUB-000001"

    # Update with higher sequence (1000 > initial 600)
    ev_id1 = generate_event_id("subscriptions", target_sub, 1000, OPERATION_UPDATE)
    payload1 = json.dumps(
        {
            "subscription_id": target_sub,
            "account_id": "ACC-000001",
            "plan": "ENTERPRISE",
            "status": "ACTIVE",
            "monthly_amount": 999.00,
            "renewal_date": "2027-01-01",
        }
    )
    raw_df1 = spark.createDataFrame(
        [
            (
                ev_id1,
                "subscriptions",
                OPERATION_UPDATE,
                target_sub,
                1000,
                "2026-03-01T10:00:00Z",
                "2026-03-01T10:00:00Z",
                1,
                CURRENT_SCHEMA_VERSION,
                payload1,
            )
        ],
        CDC_SPARK_SCHEMA,
    )
    parsed_df1 = parse_cdc_payloads(raw_df1, "subscriptions")

    audit1, _ = classify_events_and_apply_merge(
        spark, "subscriptions", parsed_df1, batch_id=1, delta_dir=delta_dir
    )
    upsert_applied_events(spark, audit1, audit_dir)

    row1 = (
        load_current_table(spark, "subscriptions", delta_dir)
        .filter(f"subscription_id = '{target_sub}'")
        .collect()[0]
    )
    # Test 17: UPDATE modifies row
    assert row1["plan"] == "ENTERPRISE"
    # Test 18: PK preserved
    assert row1["subscription_id"] == target_sub
    # Test 19: Higher sequence wins
    assert row1["_last_source_sequence"] == 1000

    # Test 20: Lower sequence (900 < 1000) cannot overwrite
    ev_id2 = generate_event_id("subscriptions", target_sub, 900, OPERATION_UPDATE)
    payload2 = json.dumps(
        {
            "subscription_id": target_sub,
            "account_id": "ACC-000001",
            "plan": "BASIC",
            "status": "ACTIVE",
            "monthly_amount": 29.00,
            "renewal_date": "2027-01-01",
        }
    )
    raw_df2 = spark.createDataFrame(
        [
            (
                ev_id2,
                "subscriptions",
                OPERATION_UPDATE,
                target_sub,
                900,
                "2026-03-01T10:00:00Z",
                "2026-03-01T10:00:00Z",
                2,
                CURRENT_SCHEMA_VERSION,
                payload2,
            )
        ],
        CDC_SPARK_SCHEMA,
    )
    parsed_df2 = parse_cdc_payloads(raw_df2, "subscriptions")

    audit2, metrics2 = classify_events_and_apply_merge(
        spark, "subscriptions", parsed_df2, batch_id=2, delta_dir=delta_dir
    )
    upsert_applied_events(spark, audit2, audit_dir)

    # Row must remain ENTERPRISE with sequence 1000
    row2 = (
        load_current_table(spark, "subscriptions", delta_dir)
        .filter(f"subscription_id = '{target_sub}'")
        .collect()[0]
    )
    assert row2["plan"] == "ENTERPRISE"
    assert row2["_last_source_sequence"] == 1000
    assert metrics2.stale_noops == 1


def test_21_to_23_multiple_same_key_events_within_batch(spark: SparkSession, temp_test_dir: Path):
    """Tests 21-23: Multiple updates to same key remain distinct in history; highest sequence wins; earlier is SUPERSEDED."""
    delta_dir, audit_dir = _setup_test_env(temp_test_dir, spark)
    sub_id = "SUB-000005"

    ev_id_low = generate_event_id("subscriptions", sub_id, 1100, OPERATION_UPDATE)
    ev_id_high = generate_event_id("subscriptions", sub_id, 1105, OPERATION_UPDATE)

    p_low = json.dumps(
        {
            "subscription_id": sub_id,
            "account_id": "ACC-1",
            "plan": "PRO",
            "status": "ACTIVE",
            "monthly_amount": 100.0,
            "renewal_date": "2027-01-01",
        }
    )
    p_high = json.dumps(
        {
            "subscription_id": sub_id,
            "account_id": "ACC-1",
            "plan": "ENTERPRISE",
            "status": "ACTIVE",
            "monthly_amount": 300.0,
            "renewal_date": "2027-01-01",
        }
    )

    raw_rows = [
        (
            ev_id_low,
            "subscriptions",
            OPERATION_UPDATE,
            sub_id,
            1100,
            "2026-03-01T10:00:00Z",
            "2026-03-01T10:00:00Z",
            1,
            CURRENT_SCHEMA_VERSION,
            p_low,
        ),
        (
            ev_id_high,
            "subscriptions",
            OPERATION_UPDATE,
            sub_id,
            1105,
            "2026-03-01T10:05:00Z",
            "2026-03-01T10:05:00Z",
            1,
            CURRENT_SCHEMA_VERSION,
            p_high,
        ),
    ]
    raw_df = spark.createDataFrame(raw_rows, CDC_SPARK_SCHEMA)
    parsed_df = parse_cdc_payloads(raw_df, "subscriptions")

    audit_df, metrics = classify_events_and_apply_merge(
        spark, "subscriptions", parsed_df, batch_id=1, delta_dir=delta_dir
    )
    upsert_applied_events(spark, audit_df, audit_dir)

    # Test 21: Both events exist in audit history
    ledger_df = (
        spark.read.format("delta")
        .load(str(get_applied_events_path(audit_dir)))
        .filter(f"business_key = '{sub_id}'")
    )
    assert ledger_df.count() == 2

    # Test 22: Highest sequence determines current state
    current_row = (
        load_current_table(spark, "subscriptions", delta_dir)
        .filter(f"subscription_id = '{sub_id}'")
        .collect()[0]
    )
    assert current_row["plan"] == "ENTERPRISE"
    assert current_row["_last_source_sequence"] == 1105

    # Test 23: Lower update is SUPERSEDED_WITHIN_BATCH
    assert metrics.superseded_events == 1
    superseded_entry = ledger_df.filter(f"event_id = '{ev_id_low}'").collect()[0]
    assert superseded_entry["apply_outcome"] == "SUPERSEDED_WITHIN_BATCH"


def test_24_to_27_delete_handling_and_tombstones(spark: SparkSession, temp_test_dir: Path):
    """Tests 24-27: DELETE sets tombstone, active helper excludes it, target seq updated, unknown key creates tombstone."""
    delta_dir, audit_dir = _setup_test_env(temp_test_dir, spark)
    target_sub = "SUB-000010"

    # Known entity DELETE
    ev_del = generate_event_id("subscriptions", target_sub, 1200, OPERATION_DELETE)
    p_del = json.dumps({"subscription_id": target_sub})
    raw_df = spark.createDataFrame(
        [
            (
                ev_del,
                "subscriptions",
                OPERATION_DELETE,
                target_sub,
                1200,
                "2026-03-01T10:00:00Z",
                "2026-03-01T10:00:00Z",
                1,
                CURRENT_SCHEMA_VERSION,
                p_del,
            )
        ],
        CDC_SPARK_SCHEMA,
    )
    parsed_df = parse_cdc_payloads(raw_df, "subscriptions")

    audit_df, _ = classify_events_and_apply_merge(
        spark, "subscriptions", parsed_df, batch_id=1, delta_dir=delta_dir
    )
    upsert_applied_events(spark, audit_df, audit_dir)

    # Test 24: Physical row remains as tombstone (_is_deleted = true)
    phys_row = (
        load_current_table(spark, "subscriptions", delta_dir)
        .filter(f"subscription_id = '{target_sub}'")
        .collect()[0]
    )
    assert phys_row["_is_deleted"] is True
    # Test 26: Target last sequence = delete sequence
    assert phys_row["_last_source_sequence"] == 1200

    # Test 25: Active view excludes tombstone
    active_rows = load_active_table(spark, "subscriptions", delta_dir).filter(
        f"subscription_id = '{target_sub}'"
    )
    assert active_rows.count() == 0

    # Test 27: Unknown key DELETE creates protective tombstone
    unknown_sub = "SUB-UNKNOWN-99"
    ev_unk = generate_event_id("subscriptions", unknown_sub, 1300, OPERATION_DELETE)
    p_unk = json.dumps({"subscription_id": unknown_sub})
    raw_unk = spark.createDataFrame(
        [
            (
                ev_unk,
                "subscriptions",
                OPERATION_DELETE,
                unknown_sub,
                1300,
                "2026-03-01T10:00:00Z",
                "2026-03-01T10:00:00Z",
                1,
                CURRENT_SCHEMA_VERSION,
                p_unk,
            )
        ],
        CDC_SPARK_SCHEMA,
    )
    parsed_unk = parse_cdc_payloads(raw_unk, "subscriptions")

    audit_unk, _ = classify_events_and_apply_merge(
        spark, "subscriptions", parsed_unk, batch_id=1, delta_dir=delta_dir
    )
    upsert_applied_events(spark, audit_unk, audit_dir)

    unk_row = (
        load_current_table(spark, "subscriptions", delta_dir)
        .filter(f"subscription_id = '{unknown_sub}'")
        .collect()[0]
    )
    assert unk_row["_is_deleted"] is True
    assert unk_row["_last_source_sequence"] == 1300
    assert unk_row["plan"] is None  # Unavailable business attributes set to NULL


def test_28_and_29_reinsert_after_delete(spark: SparkSession, temp_test_dir: Path):
    """Tests 28-29: Higher-sequence INSERT reactivates tombstone; lower-sequence INSERT is ignored."""
    delta_dir, audit_dir = _setup_test_env(temp_test_dir, spark)
    sub_id = "SUB-000020"

    # Step 1: DELETE at seq 1400
    ev_del = generate_event_id("subscriptions", sub_id, 1400, OPERATION_DELETE)
    raw_del = spark.createDataFrame(
        [
            (
                ev_del,
                "subscriptions",
                OPERATION_DELETE,
                sub_id,
                1400,
                "2026-03-01T10:00:00Z",
                "2026-03-01T10:00:00Z",
                1,
                CURRENT_SCHEMA_VERSION,
                json.dumps({"subscription_id": sub_id}),
            )
        ],
        CDC_SPARK_SCHEMA,
    )
    audit_del, _ = classify_events_and_apply_merge(
        spark, "subscriptions", parse_cdc_payloads(raw_del, "subscriptions"), 1, delta_dir=delta_dir
    )
    upsert_applied_events(spark, audit_del, audit_dir)

    # Step 2: Test 29: Lower-sequence INSERT (seq 1350 < 1400) is ignored (stale no-op)
    ev_stale = generate_event_id("subscriptions", sub_id, 1350, OPERATION_INSERT)
    p_stale = json.dumps(
        {
            "subscription_id": sub_id,
            "account_id": "ACC-1",
            "plan": "BASIC",
            "status": "ACTIVE",
            "monthly_amount": 25.0,
            "renewal_date": "2027-01-01",
        }
    )
    raw_stale = spark.createDataFrame(
        [
            (
                ev_stale,
                "subscriptions",
                OPERATION_INSERT,
                sub_id,
                1350,
                "2026-03-01T10:00:00Z",
                "2026-03-01T10:00:00Z",
                2,
                CURRENT_SCHEMA_VERSION,
                p_stale,
            )
        ],
        CDC_SPARK_SCHEMA,
    )
    audit_stale, m_stale = classify_events_and_apply_merge(
        spark,
        "subscriptions",
        parse_cdc_payloads(raw_stale, "subscriptions"),
        2,
        delta_dir=delta_dir,
    )
    upsert_applied_events(spark, audit_stale, audit_dir)
    assert m_stale.stale_noops == 1

    # Row must remain deleted tombstone at seq 1400
    row_del = (
        load_current_table(spark, "subscriptions", delta_dir)
        .filter(f"subscription_id = '{sub_id}'")
        .collect()[0]
    )
    assert row_del["_is_deleted"] is True
    assert row_del["_last_source_sequence"] == 1400

    # Step 3: Test 28: Higher-sequence INSERT (seq 1500 > 1400) reactivates record with new after-image
    ev_react = generate_event_id("subscriptions", sub_id, 1500, OPERATION_INSERT)
    p_react = json.dumps(
        {
            "subscription_id": sub_id,
            "account_id": "ACC-1",
            "plan": "ENTERPRISE",
            "status": "ACTIVE",
            "monthly_amount": 500.0,
            "renewal_date": "2027-01-01",
        }
    )
    raw_react = spark.createDataFrame(
        [
            (
                ev_react,
                "subscriptions",
                OPERATION_INSERT,
                sub_id,
                1500,
                "2026-03-01T10:00:00Z",
                "2026-03-01T10:00:00Z",
                3,
                CURRENT_SCHEMA_VERSION,
                p_react,
            )
        ],
        CDC_SPARK_SCHEMA,
    )
    audit_react, m_react = classify_events_and_apply_merge(
        spark,
        "subscriptions",
        parse_cdc_payloads(raw_react, "subscriptions"),
        3,
        delta_dir=delta_dir,
    )
    upsert_applied_events(spark, audit_react, audit_dir)
    assert m_react.applied_inserts == 1

    row_react = (
        load_current_table(spark, "subscriptions", delta_dir)
        .filter(f"subscription_id = '{sub_id}'")
        .collect()[0]
    )
    assert row_react["_is_deleted"] is False
    assert row_react["_last_source_sequence"] == 1500
    assert row_react["plan"] == "ENTERPRISE"
    assert (
        load_active_table(spark, "subscriptions", delta_dir)
        .filter(f"subscription_id = '{sub_id}'")
        .count()
        == 1
    )
