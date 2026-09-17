"""Unit tests for the Canonical Delta CDC Event Store (Part B).

Verifies:
7. Valid event inserted
8. Duplicate event rerun remains one row
9. Conflicting immutable event content raises EventIdentityConflictError
10. Legitimate multiple events same business key remain distinct
11. Full payload preserved
12. Backfill is idempotent
"""

import json
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from src.events.event_store import (
    EventIdentityConflictError,
    backfill_event_store,
    upsert_to_event_store,
)
from src.schemas.cdc_schema import (
    CDC_SPARK_SCHEMA,
    CURRENT_SCHEMA_VERSION,
    OPERATION_INSERT,
    OPERATION_UPDATE,
    generate_event_id,
)


def test_07_to_11_event_store_lifecycle_and_integrity(spark: SparkSession, tmp_path: Path):
    """Verify insert, idempotency, distinct changes, full payload, and conflict protection."""
    store_dir = tmp_path / "events" / "cdc_event_store"

    # 7. Valid event inserted
    sub_id = "SUB-ES-001"
    ev_1 = generate_event_id("subscriptions", sub_id, 100, OPERATION_INSERT)
    payload_1 = json.dumps(
        {
            "subscription_id": sub_id,
            "account_id": "ACC-1",
            "plan": "BASIC",
            "status": "ACTIVE",
            "monthly_amount": 29.99,
            "renewal_date": "2026-12-31",
        }
    )
    raw_df_1 = spark.createDataFrame(
        [
            (
                ev_1,
                "subscriptions",
                OPERATION_INSERT,
                sub_id,
                100,
                "2026-03-01T10:00:00Z",
                "2026-03-01T10:05:00Z",
                1,
                CURRENT_SCHEMA_VERSION,
                payload_1,
            )
        ],
        CDC_SPARK_SCHEMA,
    )

    count_1 = upsert_to_event_store(spark, raw_df_1, store_dir)
    assert count_1 == 1

    stored_df = spark.read.format("delta").load(str(store_dir))
    assert stored_df.count() == 1
    row_1 = stored_df.filter(f"event_id = '{ev_1}'").collect()[0]
    # 11. Full payload preserved
    assert row_1["payload"] == payload_1
    assert row_1["source_sequence"] == 100

    # 8. Duplicate event rerun remains one row
    upsert_to_event_store(spark, raw_df_1, store_dir)
    assert spark.read.format("delta").load(str(store_dir)).count() == 1

    # 10. Legitimate multiple events same business key remain distinct
    ev_2 = generate_event_id("subscriptions", sub_id, 150, OPERATION_UPDATE)
    payload_2 = json.dumps(
        {
            "subscription_id": sub_id,
            "account_id": "ACC-1",
            "plan": "PRO",
            "status": "ACTIVE",
            "monthly_amount": 99.99,
            "renewal_date": "2026-12-31",
        }
    )
    raw_df_2 = spark.createDataFrame(
        [
            (
                ev_2,
                "subscriptions",
                OPERATION_UPDATE,
                sub_id,
                150,
                "2026-03-01T11:00:00Z",
                "2026-03-01T11:05:00Z",
                1,
                CURRENT_SCHEMA_VERSION,
                payload_2,
            )
        ],
        CDC_SPARK_SCHEMA,
    )

    upsert_to_event_store(spark, raw_df_2, store_dir)
    assert spark.read.format("delta").load(str(store_dir)).count() == 2

    # 9. Conflicting immutable event content for existing event_id raises EventIdentityConflictError
    conflicting_raw = spark.createDataFrame(
        [
            (
                ev_1,  # Same event_id as ev_1
                "subscriptions",
                OPERATION_INSERT,
                sub_id,
                100,
                "2026-03-01T10:00:00Z",
                "2026-03-01T10:05:00Z",
                1,
                CURRENT_SCHEMA_VERSION,
                json.dumps({"subscription_id": sub_id, "corrupted": True}),  # Divergent payload!
            )
        ],
        CDC_SPARK_SCHEMA,
    )

    with pytest.raises(EventIdentityConflictError, match="conflicting event"):
        upsert_to_event_store(spark, conflicting_raw, store_dir)


def test_12_backfill_event_store_is_idempotent(spark: SparkSession, tmp_path: Path):
    """Verify that backfilling from CDC batch directories is idempotent."""
    cdc_dir = tmp_path / "cdc"
    batch_dir = cdc_dir / "batch_id=000001"
    batch_dir.mkdir(parents=True, exist_ok=True)

    ev_acc = generate_event_id("accounts", "ACC-100", 10, OPERATION_INSERT)
    payload_acc = json.dumps(
        {
            "account_id": "ACC-100",
            "company_name": "Acme Corp",
            "industry": "Technology",
            "country": "US",
            "plan_tier": "ENTERPRISE",
        }
    )
    acc_record = {
        "event_id": ev_acc,
        "source_table": "accounts",
        "operation": OPERATION_INSERT,
        "business_key": "ACC-100",
        "source_sequence": 10,
        "event_timestamp": "2026-03-01T00:00:00Z",
        "ingested_timestamp": "2026-03-01T00:01:00Z",
        "batch_id": 1,
        "schema_version": CURRENT_SCHEMA_VERSION,
        "payload": payload_acc,
    }
    with open(batch_dir / "accounts.jsonl", "w", encoding="utf-8") as f:
        f.write(json.dumps(acc_record) + "\n")

    store_dir = tmp_path / "events" / "cdc_event_store"

    # First backfill run
    cnt_1 = backfill_event_store(spark, cdc_dir, store_dir)
    assert cnt_1 == 1

    store_df = spark.read.format("delta").load(str(store_dir))
    assert store_df.count() == 1

    # Second backfill run: must remain 1 row
    cnt_2 = backfill_event_store(spark, cdc_dir, store_dir)
    assert cnt_2 == 1
    assert spark.read.format("delta").load(str(store_dir)).count() == 1
