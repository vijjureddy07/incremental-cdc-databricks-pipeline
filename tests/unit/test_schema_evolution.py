"""Unit tests for Schema Evolution (V1/V2 Registry, Validation, Controlled Migration, V2 Apply)."""

import json
from decimal import Decimal
from pathlib import Path

from pyspark.sql import SparkSession

from src.cdc.validation import validate_event_row
from src.delta.current_state import (
    initialize_delta_current_state,
    load_current_table,
)
from src.delta.merge import classify_events_and_apply_merge
from src.generation.initial_state import generate_initial_state
from src.schema_evolution.migrations import migrate_subscriptions_to_v2
from src.schema_evolution.registry import (
    SUBSCRIPTIONS_V1_SCHEMA,
    SUBSCRIPTIONS_V2_SCHEMA,
    is_supported_schema_version,
)
from src.schemas.cdc_schema import (
    OPERATION_UPDATE,
    generate_event_id,
)


def test_schema_registry_and_validation():
    """Verify Requirements 38, 39, 40: V1/V2 schemas, unsupported schema version rejection."""
    # Req 38 & 39
    v1_field_names = [f.name for f in SUBSCRIPTIONS_V1_SCHEMA.fields]
    assert "subscription_id" in v1_field_names
    assert "billing_cycle" not in v1_field_names
    assert "currency" not in v1_field_names

    v2_field_names = [f.name for f in SUBSCRIPTIONS_V2_SCHEMA.fields]
    assert "subscription_id" in v2_field_names
    assert "billing_cycle" in v2_field_names
    assert "currency" in v2_field_names

    assert is_supported_schema_version("subscriptions", 1) is True
    assert is_supported_schema_version("subscriptions", 2) is True
    assert is_supported_schema_version("subscriptions", 99) is False
    assert is_supported_schema_version("accounts", 2) is False

    # Req 40: Unsupported schema version quarantined with UNSUPPORTED_SCHEMA_VERSION
    sub_id = "SUB-000001"
    seq = 500
    ev_id = generate_event_id("subscriptions", sub_id, seq, OPERATION_UPDATE)
    payload = json.dumps(
        {"subscription_id": sub_id, "account_id": "ACC-000001", "plan": "PRO", "status": "ACTIVE"}
    )

    is_valid, reason = validate_event_row(
        event_id=ev_id,
        source_table="subscriptions",
        operation=OPERATION_UPDATE,
        business_key=sub_id,
        source_sequence=seq,
        event_timestamp="2026-02-01T12:00:00+00:00",
        batch_id=1,
        schema_version=99,  # Unknown version!
        payload=payload,
    )
    assert is_valid is False
    assert "UNSUPPORTED_SCHEMA_VERSION" in reason


def test_controlled_migration_and_v2_merge(spark: SparkSession, tmp_path: Path):
    """Verify Requirements 41, 42, 43, 44, 45: Controlled Delta migration, NULL preservation, V2 apply, _last_schema_version."""
    sample_dir = tmp_path / "sample_data"
    delta_dir = tmp_path / "delta"
    audit_dir = delta_dir / "audit"
    generate_initial_state(scale="tiny", seed=42, output_dir=sample_dir / "snapshot")

    # Initialize current state (V1 snapshot)
    counts = initialize_delta_current_state(spark, snapshot_dir=sample_dir, delta_dir=delta_dir)
    assert counts["subscriptions"] > 0

    sub_df_before = load_current_table(spark, "subscriptions", delta_dir)
    assert "billing_cycle" not in sub_df_before.columns
    assert "currency" not in sub_df_before.columns
    # Check snapshot rows have _last_schema_version = 1
    sample_row = sub_df_before.first()
    assert sample_row["_last_schema_version"] == 1

    # Req 41, 42: Migrate to V2
    mig_res = migrate_subscriptions_to_v2(spark, delta_dir)
    assert mig_res["status"] == "MIGRATED_SUCCESS"
    assert "billing_cycle" in mig_res["columns"]
    assert "currency" in mig_res["columns"]

    # Idempotent migration
    mig_res2 = migrate_subscriptions_to_v2(spark, delta_dir)
    assert mig_res2["status"] == "ALREADY_MIGRATED"

    # Req 43: Existing V1 rows retain NULL for new fields
    sub_df_migrated = load_current_table(spark, "subscriptions", delta_dir)
    null_v2_count = sub_df_migrated.filter(
        sub_df_migrated.billing_cycle.isNull() & sub_df_migrated.currency.isNull()
    ).count()
    assert null_v2_count == counts["subscriptions"]

    # Req 44, 45: Apply a V2 event populating new fields and updating _last_schema_version
    target_sub_id = sample_row["subscription_id"]
    current_seq = sample_row["_last_source_sequence"]
    new_seq = current_seq + 100

    v2_event_id = generate_event_id("subscriptions", target_sub_id, new_seq, OPERATION_UPDATE)
    v2_payload = json.dumps(
        {
            "subscription_id": target_sub_id,
            "account_id": sample_row["account_id"],
            "plan": "ENTERPRISE",
            "status": "ACTIVE",
            "monthly_amount": 499.00,
            "renewal_date": "2027-01-01",
            "billing_cycle": "ANNUAL",
            "currency": "USD",
        }
    )

    v2_event = {
        "event_id": v2_event_id,
        "source_table": "subscriptions",
        "operation": OPERATION_UPDATE,
        "business_key": target_sub_id,
        "source_sequence": new_seq,
        "event_timestamp": "2026-02-02T10:00:00+00:00",
        "ingested_timestamp": "2026-02-02T10:00:00+00:00",
        "batch_id": 4,
        "schema_version": 2,
        "payload": v2_payload,
        "subscription_id": target_sub_id,
        "account_id": sample_row["account_id"],
        "plan": "ENTERPRISE",
        "status": "ACTIVE",
        "monthly_amount": Decimal("499.00"),
        "renewal_date": "2027-01-01",
        "billing_cycle": "ANNUAL",
        "currency": "USD",
    }

    # Create Spark DataFrame for candidate event
    from pyspark.sql.types import (
        DecimalType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    v2_spark_schema = StructType(
        [
            StructField("event_id", StringType(), False),
            StructField("source_table", StringType(), False),
            StructField("operation", StringType(), False),
            StructField("business_key", StringType(), False),
            StructField("source_sequence", LongType(), False),
            StructField("event_timestamp", StringType(), False),
            StructField("ingested_timestamp", StringType(), True),
            StructField("batch_id", LongType(), False),
            StructField("schema_version", IntegerType(), False),
            StructField("payload", StringType(), False),
            StructField("subscription_id", StringType(), False),
            StructField("account_id", StringType(), True),
            StructField("plan", StringType(), True),
            StructField("status", StringType(), True),
            StructField("monthly_amount", DecimalType(12, 2), True),
            StructField("renewal_date", StringType(), True),
            StructField("billing_cycle", StringType(), True),
            StructField("currency", StringType(), True),
        ]
    )

    v2_df = spark.createDataFrame([v2_event], schema=v2_spark_schema)

    _, metrics = classify_events_and_apply_merge(
        spark=spark,
        parsed_batch_df=v2_df,
        source_table="subscriptions",
        batch_id=4,
        delta_dir=delta_dir,
        audit_dir=audit_dir,
    )
    assert metrics.applied_updates == 1

    # Verify updated row
    updated_row = (
        load_current_table(spark, "subscriptions", delta_dir)
        .filter(f"subscription_id = '{target_sub_id}'")
        .first()
    )
    assert updated_row["plan"] == "ENTERPRISE"
    assert updated_row["billing_cycle"] == "ANNUAL"
    assert updated_row["currency"] == "USD"
    assert updated_row["_last_schema_version"] == 2
    assert updated_row["_last_source_sequence"] == new_seq
