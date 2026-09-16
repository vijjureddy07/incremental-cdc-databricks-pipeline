"""Unit tests for deterministic deduplication and change ordering."""

import json

from pyspark.sql import SparkSession

from src.cdc.deduplication import deduplicate_cdc_events
from src.cdc.ordering import order_cdc_events
from src.schemas.cdc_schema import (
    CDC_SPARK_SCHEMA,
    CURRENT_SCHEMA_VERSION,
    OPERATION_UPDATE,
    generate_event_id,
)


def test_11_duplicate_event_detected(spark: SparkSession):
    """Test 11: Duplicate event with identical event_id is detected and separated."""
    ev_id = generate_event_id("subscriptions", "SUB-000001", 50, OPERATION_UPDATE)
    payload = json.dumps(
        {"subscription_id": "SUB-000001", "account_id": "ACC-1", "plan": "PRO", "status": "ACTIVE"}
    )

    ev1 = (
        ev_id,
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
    ev2 = (
        ev_id,
        "subscriptions",
        OPERATION_UPDATE,
        "SUB-000001",
        50,
        "2026-02-01T10:00:00Z",
        "2026-02-01T10:06:00Z",
        1,
        CURRENT_SCHEMA_VERSION,
        payload,
    )

    df = spark.createDataFrame([ev1, ev2], CDC_SPARK_SCHEMA)
    deduped_df, duplicates_df = deduplicate_cdc_events(df)

    assert deduped_df.count() == 1
    assert duplicates_df.count() == 1
    assert duplicates_df.collect()[0]["event_id"] == ev_id


def test_12_deterministic_duplicate_selection(spark: SparkSession):
    """Test 12: ROW_NUMBER selection is strictly deterministic (keeps earlier ingested timestamp)."""
    ev_id = generate_event_id("subscriptions", "SUB-000001", 50, OPERATION_UPDATE)
    payload1 = json.dumps(
        {
            "subscription_id": "SUB-000001",
            "account_id": "ACC-1",
            "plan": "PRO",
            "status": "ACTIVE",
            "tag": "FIRST",
        }
    )
    payload2 = json.dumps(
        {
            "subscription_id": "SUB-000001",
            "account_id": "ACC-1",
            "plan": "PRO",
            "status": "ACTIVE",
            "tag": "SECOND",
        }
    )

    ev1 = (
        ev_id,
        "subscriptions",
        OPERATION_UPDATE,
        "SUB-000001",
        50,
        "2026-02-01T10:00:00Z",
        "2026-02-01T10:01:00Z",
        1,
        CURRENT_SCHEMA_VERSION,
        payload1,
    )
    ev2 = (
        ev_id,
        "subscriptions",
        OPERATION_UPDATE,
        "SUB-000001",
        50,
        "2026-02-01T10:00:00Z",
        "2026-02-01T10:05:00Z",
        1,
        CURRENT_SCHEMA_VERSION,
        payload2,
    )

    df = spark.createDataFrame([ev2, ev1], CDC_SPARK_SCHEMA)  # Inverted arrival order
    deduped_df, _ = deduplicate_cdc_events(df)

    selected = deduped_df.collect()[0]
    assert selected["ingested_timestamp"] == "2026-02-01T10:01:00Z"
    assert json.loads(selected["payload"])["tag"] == "FIRST"


def test_13_two_legitimate_updates_same_key_remain_distinct(spark: SparkSession):
    """Test 13: Multiple sequential updates to the same business key are NOT treated as duplicates."""
    sub_id = "SUB-000099"
    # Update 1: Sequence 15 (plan -> PRO)
    id1 = generate_event_id("subscriptions", sub_id, 15, OPERATION_UPDATE)
    p1 = json.dumps(
        {"subscription_id": sub_id, "account_id": "ACC-1", "plan": "PRO", "status": "ACTIVE"}
    )
    ev1 = (
        id1,
        "subscriptions",
        OPERATION_UPDATE,
        sub_id,
        15,
        "2026-02-01T10:00:00Z",
        "2026-02-01T10:01:00Z",
        1,
        CURRENT_SCHEMA_VERSION,
        p1,
    )

    # Update 2: Sequence 16 (plan -> ENTERPRISE)
    id2 = generate_event_id("subscriptions", sub_id, 16, OPERATION_UPDATE)
    p2 = json.dumps(
        {"subscription_id": sub_id, "account_id": "ACC-1", "plan": "ENTERPRISE", "status": "ACTIVE"}
    )
    ev2 = (
        id2,
        "subscriptions",
        OPERATION_UPDATE,
        sub_id,
        16,
        "2026-02-01T10:05:00Z",
        "2026-02-01T10:06:00Z",
        1,
        CURRENT_SCHEMA_VERSION,
        p2,
    )

    df = spark.createDataFrame([ev1, ev2], CDC_SPARK_SCHEMA)
    deduped_df, duplicates_df = deduplicate_cdc_events(df)

    assert deduped_df.count() == 2
    assert duplicates_df.count() == 0


def test_14_events_ordered_by_source_sequence(spark: SparkSession):
    """Test 14: Events are ordered by source_sequence regardless of physical arrival order."""
    sub_id = "SUB-000001"
    evs = []
    # Feed out of order: seq 102, seq 100, seq 105, seq 101
    for seq in [102, 100, 105, 101]:
        e_id = generate_event_id("subscriptions", sub_id, seq, OPERATION_UPDATE)
        p = json.dumps(
            {"subscription_id": sub_id, "account_id": "ACC-1", "plan": "PRO", "status": "ACTIVE"}
        )
        evs.append(
            (
                e_id,
                "subscriptions",
                OPERATION_UPDATE,
                sub_id,
                seq,
                "2026-02-01T10:00:00Z",
                "2026-02-01T10:00:00Z",
                1,
                CURRENT_SCHEMA_VERSION,
                p,
            )
        )

    df = spark.createDataFrame(evs, CDC_SPARK_SCHEMA)
    ordered_df = order_cdc_events(df)

    sorted_seqs = [r["source_sequence"] for r in ordered_df.collect()]
    assert sorted_seqs == [100, 101, 102, 105]


def test_15_out_of_order_arrival_detected(spark: SparkSession):
    """Test 15: Out-of-order physical arrival is detected by comparing input row order with sorted order."""
    sub_id = "SUB-000001"
    seqs_in_arrival_order = [102, 100, 101]
    evs = []
    for seq in seqs_in_arrival_order:
        e_id = generate_event_id("subscriptions", sub_id, seq, OPERATION_UPDATE)
        p = json.dumps(
            {"subscription_id": sub_id, "account_id": "ACC-1", "plan": "PRO", "status": "ACTIVE"}
        )
        evs.append(
            (
                e_id,
                "subscriptions",
                OPERATION_UPDATE,
                sub_id,
                seq,
                "2026-02-01T10:00:00Z",
                "2026-02-01T10:00:00Z",
                1,
                CURRENT_SCHEMA_VERSION,
                p,
            )
        )

    df = spark.createDataFrame(evs, CDC_SPARK_SCHEMA)
    ordered_df = order_cdc_events(df)

    actual_ordered_seqs = [r["source_sequence"] for r in ordered_df.collect()]
    assert seqs_in_arrival_order != actual_ordered_seqs
    assert actual_ordered_seqs == [100, 101, 102]
