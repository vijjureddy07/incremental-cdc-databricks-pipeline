"""Target schemas, lineage definitions, and audit specifications for Delta Lake current-state tables."""

from typing import Dict, List

from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# Lineage columns added to every current-state Delta table
LINEAGE_FIELD_NAMES: List[str] = [
    "_last_source_sequence",
    "_last_event_id",
    "_last_event_timestamp",
    "_last_batch_id",
    "_updated_at",
    "_is_deleted",
]

TARGET_LINEAGE_FIELDS: List[StructField] = [
    StructField("_last_source_sequence", LongType(), nullable=False),
    StructField("_last_event_id", StringType(), nullable=False),
    StructField("_last_event_timestamp", StringType(), nullable=False),
    StructField("_last_batch_id", LongType(), nullable=False),
    StructField("_updated_at", StringType(), nullable=False),
    StructField("_is_deleted", BooleanType(), nullable=False),
]

# Business Schemas (Monetary values MUST use DecimalType(12, 2))
ACCOUNTS_BUSINESS_FIELDS: List[StructField] = [
    StructField("account_id", StringType(), nullable=False),
    StructField("company_name", StringType(), nullable=True),
    StructField("industry", StringType(), nullable=True),
    StructField("country", StringType(), nullable=True),
    StructField("plan_tier", StringType(), nullable=True),
]

SUBSCRIPTIONS_BUSINESS_FIELDS: List[StructField] = [
    StructField("subscription_id", StringType(), nullable=False),
    StructField("account_id", StringType(), nullable=True),
    StructField("plan", StringType(), nullable=True),
    StructField("status", StringType(), nullable=True),
    StructField("monthly_amount", DecimalType(12, 2), nullable=True),
    StructField("renewal_date", StringType(), nullable=True),
]

INVOICES_BUSINESS_FIELDS: List[StructField] = [
    StructField("invoice_id", StringType(), nullable=False),
    StructField("account_id", StringType(), nullable=True),
    StructField("subscription_id", StringType(), nullable=True),
    StructField("amount", DecimalType(12, 2), nullable=True),
    StructField("status", StringType(), nullable=True),
    StructField("due_date", StringType(), nullable=True),
]

PAYMENTS_BUSINESS_FIELDS: List[StructField] = [
    StructField("payment_id", StringType(), nullable=False),
    StructField("invoice_id", StringType(), nullable=True),
    StructField("amount", DecimalType(12, 2), nullable=True),
    StructField("payment_status", StringType(), nullable=True),
    StructField("processor_ref", StringType(), nullable=True),
]

# StructTypes for table-specific payload parsing
PAYLOAD_SCHEMAS: Dict[str, StructType] = {
    "accounts": StructType(ACCOUNTS_BUSINESS_FIELDS),
    "subscriptions": StructType(SUBSCRIPTIONS_BUSINESS_FIELDS),
    "invoices": StructType(INVOICES_BUSINESS_FIELDS),
    "payments": StructType(PAYMENTS_BUSINESS_FIELDS),
}

# Complete current-state Delta schemas (Business columns + Lineage columns)
CURRENT_STATE_SCHEMAS: Dict[str, StructType] = {
    "accounts": StructType(ACCOUNTS_BUSINESS_FIELDS + TARGET_LINEAGE_FIELDS),
    "subscriptions": StructType(SUBSCRIPTIONS_BUSINESS_FIELDS + TARGET_LINEAGE_FIELDS),
    "invoices": StructType(INVOICES_BUSINESS_FIELDS + TARGET_LINEAGE_FIELDS),
    "payments": StructType(PAYMENTS_BUSINESS_FIELDS + TARGET_LINEAGE_FIELDS),
}

# Primary key column name per source table
TARGET_PRIMARY_KEYS: Dict[str, str] = {
    "accounts": "account_id",
    "subscriptions": "subscription_id",
    "invoices": "invoice_id",
    "payments": "payment_id",
}

# Applied Event Ledger Schema
APPLIED_EVENTS_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("source_table", StringType(), nullable=False),
        StructField("business_key", StringType(), nullable=False),
        StructField("source_sequence", LongType(), nullable=False),
        StructField("operation", StringType(), nullable=False),
        StructField("batch_id", LongType(), nullable=False),
        StructField("event_timestamp", StringType(), nullable=False),
        StructField("apply_outcome", StringType(), nullable=False),
        StructField("applied_at", StringType(), nullable=False),
        StructField("target_sequence_before", LongType(), nullable=True),
        StructField("target_sequence_after", LongType(), nullable=True),
        StructField("reason", StringType(), nullable=True),
    ]
)

# Batch Apply Metrics Schema
BATCH_APPLY_METRICS_SCHEMA = StructType(
    [
        StructField("batch_id", LongType(), nullable=False),
        StructField("source_table", StringType(), nullable=False),
        StructField("input_valid_unique_events", LongType(), nullable=False),
        StructField("superseded_events", LongType(), nullable=False),
        StructField("candidate_winners", LongType(), nullable=False),
        StructField("applied_inserts", LongType(), nullable=False),
        StructField("applied_updates", LongType(), nullable=False),
        StructField("applied_deletes", LongType(), nullable=False),
        StructField("stale_noops", LongType(), nullable=False),
        StructField("already_applied", LongType(), nullable=False),
        StructField("recovered_events", LongType(), nullable=False),
        StructField("current_active_rows", LongType(), nullable=False),
        StructField("current_deleted_rows", LongType(), nullable=False),
        StructField("min_sequence", LongType(), nullable=False),
        StructField("max_sequence", LongType(), nullable=False),
        StructField("processed_at", StringType(), nullable=False),
    ]
)

# Audit Apply Outcomes
OUTCOME_APPLIED_INSERT = "APPLIED_INSERT"
OUTCOME_APPLIED_UPDATE = "APPLIED_UPDATE"
OUTCOME_APPLIED_DELETE = "APPLIED_DELETE"
OUTCOME_SUPERSEDED_WITHIN_BATCH = "SUPERSEDED_WITHIN_BATCH"
OUTCOME_STALE_NOOP = "STALE_NOOP"
OUTCOME_ALREADY_APPLIED = "ALREADY_APPLIED"
OUTCOME_RECOVERED_AFTER_PARTIAL_COMMIT = "RECOVERED_AFTER_PARTIAL_COMMIT"
