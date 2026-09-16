"""Batch application metrics persistence for Delta Lake CDC pipelines."""

from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.config.settings import DEFAULT_AUDIT_DIR
from src.delta.merge import MergeExecutionMetrics
from src.delta.schemas import BATCH_APPLY_METRICS_SCHEMA


def get_batch_apply_metrics_path(audit_dir: Optional[Path] = None) -> Path:
    """Return local path for batch_apply_metrics Delta table."""
    base_audit = audit_dir or DEFAULT_AUDIT_DIR
    return base_audit / "batch_apply_metrics"


def initialize_batch_apply_metrics_table(
    spark: SparkSession,
    audit_dir: Optional[Path] = None,
) -> DeltaTable:
    """Initialize empty Delta table for batch apply metrics if not present."""
    table_path = get_batch_apply_metrics_path(audit_dir)
    table_path.parent.mkdir(parents=True, exist_ok=True)

    if not DeltaTable.isDeltaTable(spark, str(table_path)):
        empty_df = spark.createDataFrame([], BATCH_APPLY_METRICS_SCHEMA)
        (
            empty_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(str(table_path))
        )

    return DeltaTable.forPath(spark, str(table_path))


def record_batch_apply_metric(
    spark: SparkSession,
    batch_id: int,
    source_table: str,
    metrics: MergeExecutionMetrics,
    current_active_rows: int,
    current_deleted_rows: int,
    min_sequence: int,
    max_sequence: int,
    audit_dir: Optional[Path] = None,
) -> None:
    """Idempotently persist batch execution metrics into Delta storage.

    Keyed on (batch_id, source_table).
    """
    delta_table = initialize_batch_apply_metrics_table(spark, audit_dir)
    proc_time = datetime.now(timezone.utc).isoformat()

    row_data = [
        (
            batch_id,
            source_table,
            metrics.total_batch_events,
            metrics.superseded_events,
            metrics.candidate_winners,
            metrics.applied_inserts,
            metrics.applied_updates,
            metrics.applied_deletes,
            metrics.stale_noops,
            metrics.already_applied,
            metrics.recovered_events,
            current_active_rows,
            current_deleted_rows,
            min_sequence,
            max_sequence,
            proc_time,
        )
    ]

    metric_df = spark.createDataFrame(row_data, BATCH_APPLY_METRICS_SCHEMA)

    all_fields = [f.name for f in BATCH_APPLY_METRICS_SCHEMA.fields]
    set_dict = {f: f"source.{f}" for f in all_fields}

    (
        delta_table.alias("target")
        .merge(
            source=metric_df.alias("source"),
            condition="target.batch_id = source.batch_id AND target.source_table = source.source_table",
        )
        .whenMatchedUpdate(set=set_dict)
        .whenNotMatchedInsert(values=set_dict)
        .execute()
    )
