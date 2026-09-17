"""Schema Version Registry for version-aware CDC payload interpretation."""

from typing import Dict, List, Optional, Tuple

from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

from src.delta.schemas import (
    ACCOUNTS_BUSINESS_FIELDS,
    INVOICES_BUSINESS_FIELDS,
    PAYMENTS_BUSINESS_FIELDS,
    SUBSCRIPTIONS_BUSINESS_FIELDS,
)

# V1 Schemas
ACCOUNTS_V1_SCHEMA = StructType(ACCOUNTS_BUSINESS_FIELDS)
INVOICES_V1_SCHEMA = StructType(INVOICES_BUSINESS_FIELDS)
PAYMENTS_V1_SCHEMA = StructType(PAYMENTS_BUSINESS_FIELDS)
SUBSCRIPTIONS_V1_SCHEMA = StructType(SUBSCRIPTIONS_BUSINESS_FIELDS)

# Subscription V2 fields (Adds billing_cycle and currency)
SUBSCRIPTION_V2_ADDITIONAL_FIELDS: List[StructField] = [
    StructField("billing_cycle", StringType(), nullable=True),
    StructField("currency", StringType(), nullable=True),
]

SUBSCRIPTIONS_V2_SCHEMA = StructType(
    SUBSCRIPTIONS_BUSINESS_FIELDS + SUBSCRIPTION_V2_ADDITIONAL_FIELDS
)

# Allowed values for V2 enumerated attributes
ALLOWED_BILLING_CYCLES = {"MONTHLY", "ANNUAL"}
ALLOWED_CURRENCIES = {"USD", "INR", "EUR", "GBP"}

# Explicit table + version registry
PAYLOAD_SCHEMA_REGISTRY: Dict[Tuple[str, int], StructType] = {
    ("subscriptions", 1): SUBSCRIPTIONS_V1_SCHEMA,
    ("subscriptions", 2): SUBSCRIPTIONS_V2_SCHEMA,
    ("accounts", 1): ACCOUNTS_V1_SCHEMA,
    ("invoices", 1): INVOICES_V1_SCHEMA,
    ("payments", 1): PAYMENTS_V1_SCHEMA,
}


def is_supported_schema_version(source_table: Optional[str], schema_version: Optional[int]) -> bool:
    """Validate if a specific table and schema version pair is officially supported."""
    if not source_table or schema_version is None:
        return False
    return (source_table, schema_version) in PAYLOAD_SCHEMA_REGISTRY


def get_payload_schema(source_table: str, schema_version: int) -> Optional[StructType]:
    """Retrieve the StructType for a specific source table and schema version."""
    return PAYLOAD_SCHEMA_REGISTRY.get((source_table, schema_version))
