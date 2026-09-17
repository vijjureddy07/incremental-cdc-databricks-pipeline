"""Late Event Replay Queue management using Delta Lake."""

from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.config.settings import DEFAULT_REPLAY_DIR
from src.delta.current_state import is_delta_table_initialized

STATUS_PENDING = "PENDING"
STATUS_APPLIED = "APPLIED"
STATUS_NOOP = "NOOP"
STATUS_CONFLICT = "CONFLICT"
STATUS_FAILED = "FAILED"

LATE_EVENT_QUEUE_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("source_table", StringType(), nullable=False),
        StructField("business_key", StringType(), nullable=False),
        StructField("source_sequence", LongType(), nullable=False),
        StructField("arrival_batch_id", LongType(), nullable=False),
        StructField("schema_version", IntegerType(), nullable=False),
        StructField("detected_at", StringType(), nullable=False),
        StructField("late_reason", StringType(), nullable=False),
        StructField("replay_status", StringType(), nullable=False),
        StructField("replay_attempt_count", IntegerType(), nullable=False),
        StructField("last_replay_at", StringType(), nullable=True),
        StructField("failure_reason", StringType(), nullable=True),
    ]
)


def get_replay_queue_path(replay_dir: Optional[Path] = None) -> Path:
    """Return local path to the late event replay queue Delta table."""
    return (replay_dir or DEFAULT_REPLAY_DIR) / "late_event_queue"


def initialize_late_event_queue(
    spark: SparkSession,
    replay_dir: Optional[Path] = None,
    force_overwrite: bool = False,
) -> Path:
    """Initialize empty late event replay queue Delta table."""
    table_path = get_replay_queue_path(replay_dir)
    table_path.parent.mkdir(parents=True, exist_ok=True)

    if not force_overwrite and is_delta_table_initialized(spark, table_path):
        return table_path

    empty_df = spark.createDataFrame([], LATE_EVENT_QUEUE_SCHEMA)
    (
        empty_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(table_path))
    )
    return table_path


def enqueue_late_events(
    spark: SparkSession,
    late_events_df: DataFrame,
    replay_dir: Optional[Path] = None,
) -> int:
    """Enqueue newly detected late events into the replay queue using Delta MERGE.

    Idempotency:
    - If event_id already exists in queue, keep existing status (do NOT reset an APPLIED or CONFLICT item to PENDING).
    - Only unrecorded late events are inserted as PENDING.

    Returns:
        Count of newly inserted pending items.
    """
    table_path = initialize_late_event_queue(spark, replay_dir)
    target_table = DeltaTable.forPath(spark, str(table_path))
    count_before = target_table.toDF().count()

    # Format incoming rows to match schema
    detect_time = datetime.now(timezone.utc).isoformat()

    cols_in_df = set(late_events_df.columns)
    projected = late_events_df.select(
        F.col("event_id").cast(StringType()),
        F.col("source_table").cast(StringType()),
        F.col("business_key").cast(StringType()),
        F.col("source_sequence").cast(LongType()),
        (F.col("batch_id") if "batch_id" in cols_in_df else F.col("arrival_batch_id"))
        .cast(LongType())
        .alias("arrival_batch_id"),
        F.col("schema_version").cast(IntegerType()),
        (F.col("detected_at") if "detected_at" in cols_in_df else F.lit(detect_time)).alias(
            "detected_at"
        ),
        (
            F.col("late_reason")
            if "late_reason" in cols_in_df
            else F.lit("SOURCE_SEQUENCE_BEHIND_TARGET")
        ).alias("late_reason"),
        F.lit(STATUS_PENDING).alias("replay_status"),
        F.lit(0).cast(IntegerType()).alias("replay_attempt_count"),
        F.lit(None).cast(StringType()).alias("last_replay_at"),
        F.lit(None).cast(StringType()).alias("failure_reason"),
    ).dropDuplicates(["event_id"])

    (
        target_table.alias("target")
        .merge(projected.alias("source"), "target.event_id = source.event_id")
        .whenNotMatchedInsertAll()
        .execute()
    )

    count_after = target_table.toDF().count()
    return count_after - count_before


def load_pending_replay_events(
    spark: SparkSession,
    replay_dir: Optional[Path] = None,
) -> DataFrame:
    """Load all queue entries with replay_status == PENDING ordered by source_sequence ASC, event_id ASC."""
    table_path = get_replay_queue_path(replay_dir)
    if not is_delta_table_initialized(spark, table_path):
        return spark.createDataFrame([], LATE_EVENT_QUEUE_SCHEMA)

    return (
        spark.read.format("delta")
        .load(str(table_path))
        .filter(F.col("replay_status") == STATUS_PENDING)
        .orderBy(F.col("source_sequence").asc(), F.col("event_id").asc())
    )


def update_queue_status(
    spark: SparkSession,
    event_id: str,
    status: str,
    failure_reason: Optional[str] = None,
    replay_dir: Optional[Path] = None,
) -> None:
    """Update replay_status, replay_attempt_count, and last_replay_at for an event_id."""
    table_path = get_replay_queue_path(replay_dir)
    target_table = DeltaTable.forPath(spark, str(table_path))
    now_str = datetime.now(timezone.utc).isoformat()

    (
        target_table.update(
            condition=F.col("event_id") == event_id,
            set={
                "replay_status": F.lit(status),
                "replay_attempt_count": F.col("replay_attempt_count") + 1,
                "last_replay_at": F.lit(now_str),
                "failure_reason": F.lit(failure_reason),
            },
        )
    )
