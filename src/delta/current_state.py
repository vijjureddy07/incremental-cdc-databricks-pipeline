"""Initial snapshot loader and Delta current-state table management."""

from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.config.settings import (
    DEFAULT_CURRENT_DIR,
    DEFAULT_SAMPLE_DATA_DIR,
    SUPPORTED_TABLES,
)
from src.delta.schemas import (
    CURRENT_STATE_SCHEMAS,
    PAYLOAD_SCHEMAS,
)

SNAPSHOT_SENTINEL_EVENT_ID = "SNAPSHOT_INIT"


class DeltaStateError(RuntimeError):
    """Raised when an existing directory is corrupt, unreadable, or not a valid Delta table."""

    pass


def get_current_table_path(table_name: str, base_dir: Optional[Path] = None) -> Path:
    """Return local filesystem path for a Delta current-state table."""
    current_dir = base_dir or DEFAULT_CURRENT_DIR
    return current_dir / table_name


def is_delta_table_initialized(spark: SparkSession, table_path: Path) -> bool:
    """Check whether a Delta table has already been initialized at the path.

    FAIL-LOUD CONTRACT:
    - Path does not exist -> return False (needs initialization).
    - Path exists and is a valid Delta table -> return True (already initialized).
    - Path exists but Delta inspection fails / is not a valid Delta table -> raise DeltaStateError.
    """
    if not table_path.exists():
        return False

    try:
        is_delta = DeltaTable.isDeltaTable(spark, str(table_path))
    except Exception as exc:
        raise DeltaStateError(
            f"Corrupt or unreadable Delta state encountered at '{table_path}': {exc}"
        ) from exc

    if not is_delta:
        raise DeltaStateError(
            f"Path exists at '{table_path}' but is not a valid Delta table. "
            "Refusing to overwrite unexpected existing state without explicit force_overwrite=True."
        )

    return True


def initialize_delta_current_state(
    spark: SparkSession,
    snapshot_dir: Optional[Path] = None,
    delta_dir: Optional[Path] = None,
    force_overwrite: bool = False,
) -> Dict[str, int]:
    """Initialize Delta current-state tables from the initial OLTP source snapshot.

    For each entity (accounts, subscriptions, invoices, payments):
    1. Reads snapshot JSONL files.
    2. Enforces explicit target schema with Decimal monetary fields.
    3. Adds audit lineage fields (_last_source_sequence, _last_event_id, _is_deleted=false, _last_schema_version=1).
    4. Writes initial Delta table to disk.

    Returns:
        Dict mapping table_name -> row_count
    """
    snap_path = (snapshot_dir or DEFAULT_SAMPLE_DATA_DIR) / "snapshot"
    dest_dir = delta_dir or DEFAULT_CURRENT_DIR
    dest_dir.mkdir(parents=True, exist_ok=True)

    counts: Dict[str, int] = {}
    init_time = datetime.now(timezone.utc).isoformat()

    for table in sorted(SUPPORTED_TABLES):
        table_delta_path = get_current_table_path(table, dest_dir)

        if not force_overwrite:
            if is_delta_table_initialized(spark, table_delta_path):
                existing_count = spark.read.format("delta").load(str(table_delta_path)).count()
                counts[table] = existing_count
                continue

        file_path = snap_path / f"{table}.jsonl"
        if not file_path.exists():
            raise FileNotFoundError(
                f"Cannot initialize Delta current state for '{table}': "
                f"snapshot file not found at {file_path}. Run generate-snapshot first."
            )

        # Snapshot reader schema (business payload fields + created_at + source_sequence)
        base_schema = PAYLOAD_SCHEMAS[table]
        snap_read_fields = list(base_schema.fields) + [
            StructField("created_at", StringType(), True),
            StructField("source_sequence", LongType(), False),
        ]
        read_schema = StructType(snap_read_fields)

        raw_snap_df = spark.read.schema(read_schema).json(str(file_path))

        # Project typed business columns and lineage metadata
        target_schema = CURRENT_STATE_SCHEMAS[table]
        projected_cols = []

        for field in target_schema.fields:
            name = field.name
            if name == "_last_source_sequence":
                projected_cols.append(F.col("source_sequence").cast(LongType()).alias(name))
            elif name == "_last_event_id":
                projected_cols.append(F.lit(SNAPSHOT_SENTINEL_EVENT_ID).alias(name))
            elif name == "_last_event_timestamp":
                projected_cols.append(F.coalesce(F.col("created_at"), F.lit(init_time)).alias(name))
            elif name == "_last_batch_id":
                projected_cols.append(F.lit(0).cast(LongType()).alias(name))
            elif name == "_updated_at":
                projected_cols.append(F.lit(init_time).alias(name))
            elif name == "_is_deleted":
                projected_cols.append(F.lit(False).alias(name))
            elif name == "_last_schema_version":
                projected_cols.append(F.lit(1).cast(IntegerType()).alias(name))
            elif isinstance(field.dataType, DecimalType):
                # Ensure monetary values are exact DecimalType(12, 2)
                projected_cols.append(F.col(name).cast(DecimalType(12, 2)).alias(name))
            else:
                projected_cols.append(F.col(name).cast(field.dataType).alias(name))

        current_df = raw_snap_df.select(*projected_cols)
        row_count = current_df.count()

        # Write Delta current-state table
        (
            current_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(str(table_delta_path))
        )

        counts[table] = row_count

    return counts


def load_current_table(
    spark: SparkSession,
    table_name: str,
    base_dir: Optional[Path] = None,
) -> DataFrame:
    """Load the full physical current-state Delta table, including soft-deleted tombstone rows."""
    table_path = get_current_table_path(table_name, base_dir)
    return spark.read.format("delta").load(str(table_path))


def load_active_table(
    spark: SparkSession,
    table_name: str,
    base_dir: Optional[Path] = None,
) -> DataFrame:
    """Load only active (non-deleted) business records from the current-state Delta table."""
    return load_current_table(spark, table_name, base_dir).filter(F.col("_is_deleted") == False)  # noqa: E712


def load_active_accounts(spark: SparkSession, base_dir: Optional[Path] = None) -> DataFrame:
    return load_active_table(spark, "accounts", base_dir)


def load_active_subscriptions(spark: SparkSession, base_dir: Optional[Path] = None) -> DataFrame:
    return load_active_table(spark, "subscriptions", base_dir)


def load_active_invoices(spark: SparkSession, base_dir: Optional[Path] = None) -> DataFrame:
    return load_active_table(spark, "invoices", base_dir)


def load_active_payments(spark: SparkSession, base_dir: Optional[Path] = None) -> DataFrame:
    return load_active_table(spark, "payments", base_dir)
