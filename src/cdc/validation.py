"""CDC Event Schema enforcement and validation engine."""

import json
from datetime import datetime, timezone
from typing import Optional, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    StringType,
    StructField,
    StructType,
)

from src.schemas.cdc_schema import (
    ALLOWED_OPERATIONS,
    ALLOWED_TABLES,
    CDC_SPARK_SCHEMA,
    OPERATION_DELETE,
    PRIMARY_KEY_BY_TABLE,
    REQUIRED_FIELDS_BY_TABLE,
    generate_event_id,
)


def validate_event_row(
    event_id: Optional[str],
    source_table: Optional[str],
    operation: Optional[str],
    business_key: Optional[str],
    source_sequence: Optional[int],
    event_timestamp: Optional[str],
    batch_id: Optional[int],
    schema_version: Optional[int],
    payload: Optional[str],
) -> Tuple[bool, Optional[str]]:
    """Validate a single CDC event envelope and payload.

    Returns:
        (is_valid: bool, rejection_reason: Optional[str])
    """
    reasons = []

    # 1. event_id checks
    if not event_id or len(event_id.strip()) == 0:
        reasons.append("MISSING_EVENT_ID")
    elif len(event_id) != 64:
        reasons.append("INVALID_EVENT_ID_FORMAT")

    # 2. source_table checks
    if not source_table or source_table not in ALLOWED_TABLES:
        reasons.append(f"UNKNOWN_SOURCE_TABLE: '{source_table}'")

    # 3. operation checks
    if not operation or operation not in ALLOWED_OPERATIONS:
        reasons.append(f"UNKNOWN_OPERATION: '{operation}'")

    # 4. business_key checks
    if not business_key or len(business_key.strip()) == 0:
        reasons.append("MISSING_BUSINESS_KEY")

    # 5. source_sequence checks
    if source_sequence is None or source_sequence <= 0:
        reasons.append(f"INVALID_SOURCE_SEQUENCE: {source_sequence}")

    # 6. event_timestamp checks
    if not event_timestamp:
        reasons.append("MISSING_EVENT_TIMESTAMP")
    else:
        try:
            datetime.fromisoformat(event_timestamp)
        except Exception:
            reasons.append(f"UNPARSEABLE_EVENT_TIMESTAMP: '{event_timestamp}'")

    # 7. batch_id checks
    if batch_id is None or batch_id <= 0:
        reasons.append(f"INVALID_BATCH_ID: {batch_id}")

    # 8. schema_version checks
    if schema_version is None or schema_version <= 0:
        reasons.append(f"INVALID_SCHEMA_VERSION: {schema_version}")

    # 9. payload validation & primary key consistency (A5)
    if payload is None or len(payload.strip()) == 0:
        reasons.append("MISSING_PAYLOAD")
    else:
        try:
            parsed_payload = json.loads(payload)
            if not isinstance(parsed_payload, dict):
                reasons.append("PAYLOAD_NOT_A_JSON_OBJECT")
            elif source_table in ALLOWED_TABLES and operation in ALLOWED_OPERATIONS:
                pk_field = PRIMARY_KEY_BY_TABLE[source_table]

                # A5: Payload PK must equal envelope business_key
                payload_pk = parsed_payload.get(pk_field)
                if payload_pk is not None and str(payload_pk) != str(business_key):
                    reasons.append(
                        f"PAYLOAD_KEY_MISMATCH: payload.{pk_field} ('{payload_pk}') != business_key ('{business_key}')"
                    )

                if operation == OPERATION_DELETE:
                    # DELETE allows reduced tombstone payload with at least the business key or primary key
                    if (
                        pk_field not in parsed_payload
                        and business_key not in parsed_payload.values()
                    ):
                        reasons.append(f"DELETE_PAYLOAD_MISSING_PRIMARY_KEY: '{pk_field}'")
                else:
                    # INSERT / UPDATE require entity-specific mandatory business fields
                    req_fields = REQUIRED_FIELDS_BY_TABLE[source_table]
                    missing = [f for f in req_fields if f not in parsed_payload]
                    if missing:
                        reasons.append(f"PAYLOAD_MISSING_REQUIRED_FIELDS: {missing}")
        except json.JSONDecodeError as exc:
            reasons.append(f"MALFORMED_JSON_PAYLOAD: {exc.msg}")

    # 10. event_id integrity verification (A4)
    # Only recompute when the component envelope fields are themselves valid
    if (
        event_id
        and len(event_id) == 64
        and source_table in ALLOWED_TABLES
        and operation in ALLOWED_OPERATIONS
        and business_key
        and len(business_key.strip()) > 0
        and source_sequence is not None
        and source_sequence > 0
    ):
        expected_event_id = generate_event_id(
            source_table=source_table,
            business_key=business_key,
            source_sequence=source_sequence,
            operation=operation,
        )
        if event_id != expected_event_id:
            reasons.append(
                f"EVENT_ID_MISMATCH: supplied '{event_id}' != expected '{expected_event_id}'"
            )

    if reasons:
        return False, "; ".join(reasons)
    return True, None


validation_return_schema = StructType(
    [
        StructField("is_valid", BooleanType(), False),
        StructField("rejection_reason", StringType(), True),
    ]
)


def validate_cdc_dataframe(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Validate DataFrame adhering to CDC_SPARK_SCHEMA.

    Returns:
        (valid_df, quarantine_df)
    """
    proc_ts = datetime.now(timezone.utc).isoformat()

    validate_udf = F.udf(validate_event_row, validation_return_schema)

    # Compute validation result
    checked_df = df.withColumn(
        "_validation_result",
        validate_udf(
            F.col("event_id"),
            F.col("source_table"),
            F.col("operation"),
            F.col("business_key"),
            F.col("source_sequence"),
            F.col("event_timestamp"),
            F.col("batch_id"),
            F.col("schema_version"),
            F.col("payload"),
        ),
    )

    # Split into valid and quarantine
    valid_df = checked_df.filter(F.col("_validation_result.is_valid") == True).drop(  # noqa: E712
        "_validation_result"
    )

    quarantine_df = (
        checked_df.filter(F.col("_validation_result.is_valid") == False)  # noqa: E712
        .withColumn("rejection_reason", F.col("_validation_result.rejection_reason"))
        .withColumn("processing_timestamp", F.lit(proc_ts))
        .withColumn(
            "original_event",
            F.to_json(
                F.struct(
                    [F.col(c) for c in CDC_SPARK_SCHEMA.fieldNames() if c in checked_df.columns]
                )
            ),
        )
        .drop("_validation_result")
    )

    return valid_df, quarantine_df
