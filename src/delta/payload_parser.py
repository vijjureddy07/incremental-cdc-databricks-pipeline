"""Table-specific CDC payload parsing using built-in Spark Catalyst expressions."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.delta.schemas import PAYLOAD_SCHEMAS, TARGET_PRIMARY_KEYS


def parse_cdc_payloads(df: DataFrame, source_table: str) -> DataFrame:
    """Parse serialized JSON payload strings into typed business columns using Spark from_json.

    FULL AFTER-IMAGE CONTRACT:
    For this CDC pipeline, INSERT and UPDATE events contain full after-images representing the complete
    state of the business entity following the transaction. They are NOT delta patches. This allows
    in-batch collapsing to the latest sequence without requiring intermediate multi-hop joins.

    DELETE events carry tombstone semantics where the business key identifies the deleted record.

    PERFORMANCE NOTE:
    Uses PySpark Catalyst expression `F.from_json()` with explicit StructTypes rather than Python UDFs,
    avoiding Python/JVM serialization bottlenecks.
    """
    if source_table not in PAYLOAD_SCHEMAS:
        raise ValueError(f"Unknown source table for payload parsing: '{source_table}'")

    payload_schema = PAYLOAD_SCHEMAS[source_table]
    pk_field = TARGET_PRIMARY_KEYS[source_table]

    # Filter for the target table
    table_df = df.filter(F.col("source_table") == source_table)

    # Parse payload using built-in Catalyst from_json
    parsed_df = table_df.withColumn("_parsed", F.from_json(F.col("payload"), payload_schema))

    # Project business columns
    business_cols = []
    for field in payload_schema.fields:
        if field.name == pk_field:
            # Primary key is guaranteed present and matched by earlier validation
            business_cols.append(
                F.coalesce(F.col(f"_parsed.{field.name}"), F.col("business_key")).alias(field.name)
            )
        else:
            business_cols.append(F.col(f"_parsed.{field.name}").alias(field.name))

    # Project envelope columns
    envelope_cols = [
        "event_id",
        "source_table",
        "operation",
        "business_key",
        "source_sequence",
        "event_timestamp",
        "ingested_timestamp",
        "batch_id",
        "schema_version",
    ]

    return parsed_df.select(*envelope_cols, *business_cols)
