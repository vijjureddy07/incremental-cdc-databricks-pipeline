"""Replay Audit ledger capturing execution history and drift verification metrics."""

import hashlib
from pathlib import Path
from typing import Any, Dict, Optional

from delta.tables import DeltaTable
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.config.settings import DEFAULT_REPLAY_DIR
from src.delta.current_state import is_delta_table_initialized

REPLAY_AUDIT_SCHEMA = StructType(
    [
        StructField("replay_id", StringType(), nullable=False),
        StructField("event_id", StringType(), nullable=False),
        StructField("source_table", StringType(), nullable=False),
        StructField("business_key", StringType(), nullable=False),
        StructField("source_sequence", LongType(), nullable=False),
        StructField("status", StringType(), nullable=False),
        StructField("history_versions_before", LongType(), nullable=False),
        StructField("history_versions_after", LongType(), nullable=False),
        StructField("current_sequence_before", LongType(), nullable=False),
        StructField("current_sequence_after", LongType(), nullable=False),
        StructField("current_state_changed", BooleanType(), nullable=False),
        StructField("attempt_number", IntegerType(), nullable=False),
        StructField("started_at", StringType(), nullable=False),
        StructField("completed_at", StringType(), nullable=False),
        StructField("failure_reason", StringType(), nullable=True),
    ]
)


def generate_replay_id(event_id: str) -> str:
    """Generate deterministic replay_id for a given event."""
    return hashlib.sha256(f"late-replay:{event_id}".encode("utf-8")).hexdigest()


def get_replay_audit_path(replay_dir: Optional[Path] = None) -> Path:
    """Return local path to the replay_audit Delta table."""
    return (replay_dir or DEFAULT_REPLAY_DIR) / "replay_audit"


def initialize_replay_audit(
    spark: SparkSession,
    replay_dir: Optional[Path] = None,
    force_overwrite: bool = False,
) -> Path:
    """Initialize empty replay audit Delta table."""
    table_path = get_replay_audit_path(replay_dir)
    table_path.parent.mkdir(parents=True, exist_ok=True)

    if not force_overwrite and is_delta_table_initialized(spark, table_path):
        return table_path

    empty_df = spark.createDataFrame([], REPLAY_AUDIT_SCHEMA)
    (
        empty_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(table_path))
    )
    return table_path


def record_replay_audit(
    spark: SparkSession,
    audit_record: Dict[str, Any],
    replay_dir: Optional[Path] = None,
) -> None:
    """Record or update a replay audit execution record using Delta MERGE on replay_id."""
    table_path = initialize_replay_audit(spark, replay_dir)
    target_table = DeltaTable.forPath(spark, str(table_path))

    event_id = audit_record["event_id"]
    replay_id = audit_record.get("replay_id") or generate_replay_id(event_id)

    row = Row(
        replay_id=replay_id,
        event_id=event_id,
        source_table=audit_record["source_table"],
        business_key=audit_record["business_key"],
        source_sequence=int(audit_record["source_sequence"]),
        status=audit_record["status"],
        history_versions_before=int(audit_record["history_versions_before"]),
        history_versions_after=int(audit_record["history_versions_after"]),
        current_sequence_before=int(audit_record["current_sequence_before"]),
        current_sequence_after=int(audit_record["current_sequence_after"]),
        current_state_changed=bool(audit_record["current_state_changed"]),
        attempt_number=int(audit_record.get("attempt_number", 1)),
        started_at=audit_record["started_at"],
        completed_at=audit_record["completed_at"],
        failure_reason=audit_record.get("failure_reason"),
    )

    audit_df = spark.createDataFrame([row], schema=REPLAY_AUDIT_SCHEMA)

    (
        target_table.alias("target")
        .merge(audit_df.alias("source"), "target.replay_id = source.replay_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
