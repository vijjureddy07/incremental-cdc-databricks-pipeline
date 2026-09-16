"""Unit tests for CDC schema validation and quarantine routing."""

import json

from pyspark.sql import SparkSession

from src.cdc.validation import validate_cdc_dataframe
from src.schemas.cdc_schema import (
    CDC_SPARK_SCHEMA,
    CURRENT_SCHEMA_VERSION,
    OPERATION_DELETE,
    OPERATION_INSERT,
    OPERATION_UPDATE,
    generate_event_id,
)


def test_05_insert_event_validation(spark: SparkSession):
    """Test 5: Valid INSERT event with complete payload passes validation."""
    payload = {
        "account_id": "ACC-000001",
        "company_name": "Acme Corp",
        "industry": "Technology",
        "plan_tier": "GROWTH",
    }
    seq = 10
    ev = (
        generate_event_id("accounts", "ACC-000001", seq, OPERATION_INSERT),
        "accounts",
        OPERATION_INSERT,
        "ACC-000001",
        seq,
        "2026-02-01T12:00:00+00:00",
        "2026-02-01T12:05:00+00:00",
        1,
        CURRENT_SCHEMA_VERSION,
        json.dumps(payload),
    )
    df = spark.createDataFrame([ev], CDC_SPARK_SCHEMA)
    valid_df, quarantine_df = validate_cdc_dataframe(df)

    assert valid_df.count() == 1
    assert quarantine_df.count() == 0


def test_06_update_event_validation(spark: SparkSession):
    """Test 6: Valid UPDATE event with updated fields passes validation."""
    payload = {
        "subscription_id": "SUB-000001",
        "account_id": "ACC-000001",
        "plan": "ENTERPRISE",
        "status": "ACTIVE",
    }
    seq = 12
    ev = (
        generate_event_id("subscriptions", "SUB-000001", seq, OPERATION_UPDATE),
        "subscriptions",
        OPERATION_UPDATE,
        "SUB-000001",
        seq,
        "2026-02-01T12:10:00+00:00",
        "2026-02-01T12:15:00+00:00",
        1,
        CURRENT_SCHEMA_VERSION,
        json.dumps(payload),
    )
    df = spark.createDataFrame([ev], CDC_SPARK_SCHEMA)
    valid_df, quarantine_df = validate_cdc_dataframe(df)

    assert valid_df.count() == 1
    assert quarantine_df.count() == 0


def test_07_delete_event_validation(spark: SparkSession):
    """Test 7: Valid DELETE event with reduced tombstone payload passes validation."""
    # Reduced payload contains only the primary key
    delete_payload = {
        "account_id": "ACC-000001",
        "cancellation_reason": "Customer churn",
    }
    seq = 14
    ev = (
        generate_event_id("accounts", "ACC-000001", seq, OPERATION_DELETE),
        "accounts",
        OPERATION_DELETE,
        "ACC-000001",
        seq,
        "2026-02-01T12:20:00+00:00",
        "2026-02-01T12:25:00+00:00",
        1,
        CURRENT_SCHEMA_VERSION,
        json.dumps(delete_payload),
    )
    df = spark.createDataFrame([ev], CDC_SPARK_SCHEMA)
    valid_df, quarantine_df = validate_cdc_dataframe(df)

    assert valid_df.count() == 1
    assert quarantine_df.count() == 0


def test_08_invalid_operation_rejected(spark: SparkSession):
    """Test 8: Unknown operation (e.g. UPSERT) is quarantined."""
    payload = {"account_id": "ACC-000001", "company_name": "Acme Corp"}
    seq = 15
    ev = (
        generate_event_id("accounts", "ACC-000001", seq, "UPSERT"),
        "accounts",
        "UPSERT",  # Invalid operation
        "ACC-000001",
        seq,
        "2026-02-01T12:00:00+00:00",
        "2026-02-01T12:05:00+00:00",
        1,
        CURRENT_SCHEMA_VERSION,
        json.dumps(payload),
    )
    df = spark.createDataFrame([ev], CDC_SPARK_SCHEMA)
    valid_df, quarantine_df = validate_cdc_dataframe(df)

    assert valid_df.count() == 0
    assert quarantine_df.count() == 1
    quarantined_row = quarantine_df.collect()[0]
    assert "UNKNOWN_OPERATION" in quarantined_row["rejection_reason"]


def test_09_missing_business_key_rejected(spark: SparkSession):
    """Test 9: Missing or empty business_key is quarantined."""
    payload = {"account_id": "ACC-000001", "company_name": "Acme Corp"}
    seq = 16
    ev = (
        generate_event_id("accounts", "UNKNOWN", seq, OPERATION_INSERT),
        "accounts",
        OPERATION_INSERT,
        "",  # Empty business key
        seq,
        "2026-02-01T12:00:00+00:00",
        "2026-02-01T12:05:00+00:00",
        1,
        CURRENT_SCHEMA_VERSION,
        json.dumps(payload),
    )
    df = spark.createDataFrame([ev], CDC_SPARK_SCHEMA)
    valid_df, quarantine_df = validate_cdc_dataframe(df)

    assert valid_df.count() == 0
    assert quarantine_df.count() == 1
    assert "MISSING_BUSINESS_KEY" in quarantine_df.collect()[0]["rejection_reason"]


def test_10_malformed_payload_quarantined(spark: SparkSession):
    """Test 10: Unparseable malformed JSON payload is quarantined."""
    seq = 17
    ev = (
        generate_event_id("invoices", "INV-000001", seq, OPERATION_UPDATE),
        "invoices",
        OPERATION_UPDATE,
        "INV-000001",
        seq,
        "2026-02-01T12:00:00+00:00",
        "2026-02-01T12:05:00+00:00",
        1,
        CURRENT_SCHEMA_VERSION,
        '{"invoice_id": "INV-000001", status: broken json string...',  # Malformed JSON
    )
    df = spark.createDataFrame([ev], CDC_SPARK_SCHEMA)
    valid_df, quarantine_df = validate_cdc_dataframe(df)

    assert valid_df.count() == 0
    assert quarantine_df.count() == 1
    assert "MALFORMED_JSON_PAYLOAD" in quarantine_df.collect()[0]["rejection_reason"]
