"""Idempotent Applied Event Ledger for Delta CDC mutation auditing and crash recovery."""

from pathlib import Path
from typing import Optional, Set

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from src.config.settings import DEFAULT_AUDIT_DIR
from src.delta.schemas import APPLIED_EVENTS_SCHEMA


def get_applied_events_path(audit_dir: Optional[Path] = None) -> Path:
    """Return local path for the applied_events Delta audit table."""
    base_audit = audit_dir or DEFAULT_AUDIT_DIR
    return base_audit / "applied_events"


def initialize_applied_events_table(
    spark: SparkSession,
    audit_dir: Optional[Path] = None,
) -> DeltaTable:
    """Initialize empty Delta table for applied events ledger if not present."""
    table_path = get_applied_events_path(audit_dir)
    table_path.parent.mkdir(parents=True, exist_ok=True)

    if not DeltaTable.isDeltaTable(spark, str(table_path)):
        empty_df = spark.createDataFrame([], APPLIED_EVENTS_SCHEMA)
        (
            empty_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(str(table_path))
        )

    return DeltaTable.forPath(spark, str(table_path))


def upsert_applied_events(
    spark: SparkSession,
    audit_df: DataFrame,
    audit_dir: Optional[Path] = None,
) -> None:
    """Merge audit records into the applied_events Delta ledger keyed on event_id.

    IDEMPOTENT MERGE GUARANTEE:
    Delta MERGE on `target.event_id = source.event_id` ensures that running the same batch
    multiple times never appends duplicate audit ledger records.
    """
    if audit_df.count() == 0:
        return

    delta_table = initialize_applied_events_table(spark, audit_dir)

    all_fields = [f.name for f in APPLIED_EVENTS_SCHEMA.fields]
    set_dict = {f: f"source.{f}" for f in all_fields}

    (
        delta_table.alias("target")
        .merge(
            source=audit_df.alias("source"),
            condition="target.event_id = source.event_id",
        )
        .whenMatchedUpdate(set=set_dict)
        .whenNotMatchedInsert(values=set_dict)
        .execute()
    )


def get_recorded_ledger_event_ids(
    spark: SparkSession,
    audit_dir: Optional[Path] = None,
) -> Set[str]:
    """Retrieve existing event_ids in the applied_events ledger."""
    table_path = get_applied_events_path(audit_dir)
    if not table_path.exists() or not DeltaTable.isDeltaTable(spark, str(table_path)):
        return set()

    df = spark.read.format("delta").load(str(table_path))
    rows = df.select("event_id").collect()
    return {r["event_id"] for r in rows}
