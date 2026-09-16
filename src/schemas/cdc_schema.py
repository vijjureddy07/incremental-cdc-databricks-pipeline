"""CDC Event Envelope contract and schema definitions."""

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional, Set

from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# Canonical CDC Operations
OPERATION_INSERT = "INSERT"
OPERATION_UPDATE = "UPDATE"
OPERATION_DELETE = "DELETE"

ALLOWED_OPERATIONS: Set[str] = {OPERATION_INSERT, OPERATION_UPDATE, OPERATION_DELETE}
ALLOWED_TABLES: Set[str] = {"accounts", "subscriptions", "invoices", "payments"}
CURRENT_SCHEMA_VERSION = 1

# Explicit PySpark Schema for the CDC Envelope
CDC_SPARK_SCHEMA = StructType(
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
        StructField("payload", StringType(), True),
    ]
)

# Quarantine Schema
QUARANTINE_SPARK_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("source_table", StringType(), True),
        StructField("operation", StringType(), True),
        StructField("business_key", StringType(), True),
        StructField("source_sequence", LongType(), True),
        StructField("event_timestamp", StringType(), True),
        StructField("ingested_timestamp", StringType(), True),
        StructField("batch_id", LongType(), True),
        StructField("schema_version", IntegerType(), True),
        StructField("payload", StringType(), True),
        StructField("original_event", StringType(), False),
        StructField("rejection_reason", StringType(), False),
        StructField("processing_timestamp", StringType(), False),
    ]
)

# Required business fields per entity (for INSERT/UPDATE payloads)
REQUIRED_FIELDS_BY_TABLE: Dict[str, Set[str]] = {
    "accounts": {"account_id", "company_name", "industry", "plan_tier"},
    "subscriptions": {"subscription_id", "account_id", "plan", "status"},
    "invoices": {"invoice_id", "account_id", "subscription_id", "amount", "status"},
    "payments": {"payment_id", "invoice_id", "amount", "payment_status"},
}

PRIMARY_KEY_BY_TABLE: Dict[str, str] = {
    "accounts": "account_id",
    "subscriptions": "subscription_id",
    "invoices": "invoice_id",
    "payments": "payment_id",
}


def generate_event_id(
    source_table: str,
    business_key: str,
    source_sequence: int,
    operation: str,
) -> str:
    """Generate a deterministic SHA-256 hash identifying a unique logical CDC event.

    Same logical event -> identical event_id.
    Different sequence / operation / key -> different event_id.
    """
    raw = f"{source_table}:{business_key}:{source_sequence}:{operation}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


@dataclass
class CDCEvent:
    """Dataclass representation of a CDC envelope event."""

    event_id: str
    source_table: str
    operation: str
    business_key: str
    source_sequence: int
    event_timestamp: str
    ingested_timestamp: Optional[str]
    batch_id: int
    schema_version: int
    payload: str  # Serialized JSON string

    @classmethod
    def create(
        cls,
        source_table: str,
        operation: str,
        business_key: str,
        source_sequence: int,
        event_timestamp: str,
        batch_id: int,
        payload_dict: Dict[str, Any],
        ingested_timestamp: Optional[str] = None,
        schema_version: int = CURRENT_SCHEMA_VERSION,
        override_event_id: Optional[str] = None,
    ) -> "CDCEvent":
        """Factory method computing deterministic event_id and payload serialization."""
        event_id = override_event_id or generate_event_id(
            source_table=source_table,
            business_key=business_key,
            source_sequence=source_sequence,
            operation=operation,
        )
        payload_str = json.dumps(payload_dict, sort_keys=True)
        return cls(
            event_id=event_id,
            source_table=source_table,
            operation=operation,
            business_key=business_key,
            source_sequence=source_sequence,
            event_timestamp=event_timestamp,
            ingested_timestamp=ingested_timestamp,
            batch_id=batch_id,
            schema_version=schema_version,
            payload=payload_str,
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())
