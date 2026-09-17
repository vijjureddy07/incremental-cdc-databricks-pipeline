"""Canonical Delta CDC Event Store.

Provides durable, immutable, and idempotent persistence for all valid, deduplicated
CDC change events prior to current-state mutation or historical collapsing.
"""

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.cdc.deduplication import add_canonical_event_hash, deduplicate_cdc_events
from src.cdc.discovery import discover_batches
from src.cdc.validation import validate_cdc_dataframe
from src.config.settings import (
    DEFAULT_CDC_DATA_DIR,
    DEFAULT_CDC_EVENT_STORE_DIR,
    SUPPORTED_TABLES,
)
from src.schemas.cdc_schema import CDC_SPARK_SCHEMA

EVENT_STORE_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("source_table", StringType(), nullable=False),
        StructField("business_key", StringType(), nullable=False),
        StructField("source_sequence", LongType(), nullable=False),
        StructField("operation", StringType(), nullable=False),
        StructField("event_timestamp", StringType(), nullable=False),
        StructField("ingested_timestamp", StringType(), nullable=False),
        StructField("arrival_batch_id", LongType(), nullable=False),
        StructField("schema_version", IntegerType(), nullable=False),
        StructField("payload", StringType(), nullable=False),
        StructField("canonical_event_hash", StringType(), nullable=True),
        StructField("is_late_on_arrival", BooleanType(), nullable=False),
        StructField("first_seen_at", StringType(), nullable=False),
        StructField("last_seen_at", StringType(), nullable=False),
    ]
)


class EventIdentityConflictError(RuntimeError):
    """Raised when an incoming event shares an event_id with an existing event but differs in immutable content."""

    pass


def get_event_store_path(base_dir: Optional[Path] = None) -> Path:
    """Return local path for the canonical CDC event store Delta table."""
    return base_dir or DEFAULT_CDC_EVENT_STORE_DIR


def initialize_event_store(
    spark: SparkSession, event_store_path: Optional[Path] = None
) -> DeltaTable:
    """Initialize empty Delta table for canonical CDC event store if not present."""
    table_path = get_event_store_path(event_store_path)
    table_path.parent.mkdir(parents=True, exist_ok=True)

    if not table_path.exists() or not DeltaTable.isDeltaTable(spark, str(table_path)):
        empty_df = spark.createDataFrame([], EVENT_STORE_SCHEMA)
        (
            empty_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(str(table_path))
        )

    return DeltaTable.forPath(spark, str(table_path))


def upsert_to_event_store(
    spark: SparkSession,
    events_df: Any,
    event_store_path: Optional[Path] = None,
) -> int:
    """Idempotently persist valid deduplicated CDC events into the canonical Delta event store.

    CONTRACT & CONFLICT PROTECTION:
    1. Keyed by event_id.
    2. Repeated deliveries of the exact same event update only last_seen_at metadata.
    3. If an existing event_id is encountered with conflicting immutable fields
       (source_table, business_key, source_sequence, operation, payload, schema_version),
       raises EventIdentityConflictError.

    Returns:
        Number of events processed.
    """
    if isinstance(events_df, list):
        if not events_df:
            return 0
        from pyspark.sql import Row

        events_df = spark.createDataFrame([Row(**e) for e in events_df])
    elif events_df.count() == 0:
        return 0

    table_path = get_event_store_path(event_store_path)
    delta_table = initialize_event_store(spark, table_path)
    now_iso = datetime.now(timezone.utc).isoformat()

    # Ensure required audit lineage columns exist
    df = events_df
    if "canonical_event_hash" not in df.columns:
        df = add_canonical_event_hash(df)
    if "is_late_on_arrival" not in df.columns:
        df = df.withColumn("is_late_on_arrival", F.lit(False))
    if "first_seen_at" not in df.columns:
        df = df.withColumn("first_seen_at", F.lit(now_iso))
    if "last_seen_at" not in df.columns:
        df = df.withColumn("last_seen_at", F.lit(now_iso))
    if "arrival_batch_id" not in df.columns and "batch_id" in df.columns:
        df = df.withColumn("arrival_batch_id", F.col("batch_id").cast(LongType()))

    cols_to_select = [f.name for f in EVENT_STORE_SCHEMA.fields]
    projected_df = df.select(*cols_to_select)

    # Detect conflicts against existing events in store
    existing_store_df = spark.read.format("delta").load(str(table_path))
    if existing_store_df.count() > 0:
        conflicts_df = (
            projected_df.alias("src")
            .join(
                existing_store_df.alias("tgt"),
                F.col("src.event_id") == F.col("tgt.event_id"),
                "inner",
            )
            .filter(
                (F.col("src.source_table") != F.col("tgt.source_table"))
                | (F.col("src.business_key") != F.col("tgt.business_key"))
                | (F.col("src.source_sequence") != F.col("tgt.source_sequence"))
                | (F.col("src.operation") != F.col("tgt.operation"))
                | (F.col("src.payload") != F.col("tgt.payload"))
                | (F.col("src.schema_version") != F.col("tgt.schema_version"))
            )
        )
        conflict_count = conflicts_df.count()
        if conflict_count > 0:
            sample_ids = [
                r["event_id"] for r in conflicts_df.select("src.event_id").limit(5).collect()
            ]
            raise EventIdentityConflictError(
                f"Encountered {conflict_count} conflicting event(s) for existing event IDs: {sample_ids}. "
                "Deterministic event_id must never map to divergent immutable content!"
            )

    all_fields = [f.name for f in EVENT_STORE_SCHEMA.fields]
    insert_values = {f: f"source.{f}" for f in all_fields}
    update_values = {"last_seen_at": "source.last_seen_at"}

    (
        delta_table.alias("target")
        .merge(
            source=projected_df.alias("source"),
            condition="target.event_id = source.event_id",
        )
        .whenMatchedUpdate(set=update_values)
        .whenNotMatchedInsert(values=insert_values)
        .execute()
    )

    return projected_df.count()


def backfill_event_store(
    spark: SparkSession,
    cdc_dir: Optional[Path] = None,
    event_store_path: Optional[Path] = None,
) -> int:
    """Deterministically backfill the canonical event store from existing raw CDC batch files.

    Validates and deduplicates all raw events across batches and idempotently merges them into the store.

    Returns:
        Total count of events backfilled.
    """
    batches = discover_batches(cdc_dir or DEFAULT_CDC_DATA_DIR)
    total_upserted = 0

    for batch in batches:
        for table in sorted(SUPPORTED_TABLES):
            file_path = batch.table_files.get(table)
            if not file_path or not file_path.exists():
                continue

            raw_df = spark.read.schema(CDC_SPARK_SCHEMA).json(str(file_path))
            if raw_df.count() == 0:
                continue

            valid_df, _ = validate_cdc_dataframe(raw_df)
            deduped_df, _ = deduplicate_cdc_events(valid_df)

            if deduped_df.count() > 0:
                upserted = upsert_to_event_store(
                    spark=spark,
                    events_df=deduped_df,
                    event_store_path=event_store_path,
                )
                total_upserted += upserted

    return total_upserted
