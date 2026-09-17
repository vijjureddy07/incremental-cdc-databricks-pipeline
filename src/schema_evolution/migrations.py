"""Controlled schema evolution migrations for Delta Lake tables."""

import logging
from pathlib import Path
from typing import Dict, Optional

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

from src.config.settings import DEFAULT_CURRENT_DIR
from src.delta.current_state import (
    DeltaStateError,
    get_current_table_path,
    is_delta_table_initialized,
)

logger = logging.getLogger(__name__)


def migrate_subscriptions_to_v2(
    spark: SparkSession,
    delta_dir: Optional[Path] = None,
) -> Dict[str, str]:
    """Perform a controlled, explicit schema migration on the subscriptions Delta table to Schema V2.

    Controlled Evolution Rules:
    - Adds `billing_cycle` (STRING) and `currency` (STRING).
    - Existing V1 rows retain NULL for both fields.
    - Idempotent: safe to run multiple times without duplicating or corrupting fields.
    - NEVER globally enables spark.databricks.delta.schema.autoMerge.enabled.

    Returns:
        Dict with status and details of the migration.
    """
    table_path = get_current_table_path("subscriptions", delta_dir or DEFAULT_CURRENT_DIR)

    if not is_delta_table_initialized(spark, table_path):
        raise DeltaStateError(
            f"Cannot migrate subscriptions table at '{table_path}': table is not initialized."
        )

    current_df = spark.read.format("delta").load(str(table_path))
    existing_cols = set(current_df.columns)

    needed_fields = [
        StructField("billing_cycle", StringType(), nullable=True),
        StructField("currency", StringType(), nullable=True),
    ]

    missing_fields = [f for f in needed_fields if f.name not in existing_cols]

    if not missing_fields:
        return {
            "status": "ALREADY_MIGRATED",
            "message": "Subscriptions table already contains schema V2 columns (billing_cycle, currency).",
            "columns": list(current_df.columns),
        }

    # Execute controlled migration using Spark SQL ALTER TABLE or explicit mergeSchema
    try:
        alter_cols = ", ".join([f"{f.name} STRING" for f in missing_fields])
        spark.sql(f"ALTER TABLE delta.`{table_path}` ADD COLUMNS ({alter_cols})")
        logger.info("Migrated subscriptions table via SQL ALTER TABLE: added %s", alter_cols)
    except Exception as exc:
        logger.warning(
            "SQL ALTER TABLE failed (%s), falling back to controlled mergeSchema append",
            exc,
        )
        # Fallback: empty DataFrame with missing columns merged explicitly into the Delta table
        full_new_schema = StructType(current_df.schema.fields + missing_fields)
        empty_df = spark.createDataFrame([], full_new_schema)
        (
            empty_df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(str(table_path))
        )

    evolved_df = spark.read.format("delta").load(str(table_path))
    return {
        "status": "MIGRATED_SUCCESS",
        "message": f"Successfully added columns: {[f.name for f in missing_fields]}",
        "columns": list(evolved_df.columns),
    }
