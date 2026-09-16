"""Spark SQL diagnostic queries and analysis for CDC change feeds."""

from pathlib import Path
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession

from src.config.settings import (
    DEFAULT_LATE_DIR,
    DEFAULT_QUARANTINE_DIR,
    DEFAULT_VALID_DIR,
)
from src.schemas.cdc_schema import CDC_SPARK_SCHEMA, QUARANTINE_SPARK_SCHEMA
from src.utils.spark import get_spark_session


class CDCAnalyzer:
    """Performs Spark SQL diagnostic queries across processed CDC outputs."""

    def __init__(
        self,
        spark: Optional[SparkSession] = None,
        valid_dir: Optional[Path] = None,
        quarantine_dir: Optional[Path] = None,
        late_dir: Optional[Path] = None,
    ):
        self.spark = spark or get_spark_session()
        self.valid_dir = valid_dir or DEFAULT_VALID_DIR
        self.quarantine_dir = quarantine_dir or DEFAULT_QUARANTINE_DIR
        self.late_dir = late_dir or DEFAULT_LATE_DIR

    def register_views(self) -> Dict[str, DataFrame]:
        """Register output directories as Spark temporary SQL views."""
        views: Dict[str, DataFrame] = {}

        # 1. Valid Events View
        valid_files = list(self.valid_dir.glob("*/*.jsonl"))
        if valid_files:
            valid_paths = [str(p) for p in valid_files]
            df_valid = self.spark.read.schema(CDC_SPARK_SCHEMA).json(valid_paths)
        else:
            df_valid = self.spark.createDataFrame([], CDC_SPARK_SCHEMA)
        df_valid.createOrReplaceTempView("v_cdc_valid")
        views["v_cdc_valid"] = df_valid

        # 2. Quarantine View
        quarantine_files = list(self.quarantine_dir.glob("*/*.jsonl"))
        if quarantine_files:
            quarantine_paths = [str(p) for p in quarantine_files]
            df_quarantine = self.spark.read.schema(QUARANTINE_SPARK_SCHEMA).json(quarantine_paths)
        else:
            df_quarantine = self.spark.createDataFrame([], QUARANTINE_SPARK_SCHEMA)
        df_quarantine.createOrReplaceTempView("v_cdc_quarantine")
        views["v_cdc_quarantine"] = df_quarantine

        # 3. Late Events View
        late_files = list(self.late_dir.glob("*/*.jsonl"))
        if late_files:
            late_paths = [str(p) for p in late_files]
            df_late = self.spark.read.schema(CDC_SPARK_SCHEMA).json(late_paths)
        else:
            df_late = self.spark.createDataFrame([], CDC_SPARK_SCHEMA)
        df_late.createOrReplaceTempView("v_cdc_late")
        views["v_cdc_late"] = df_late

        return views

    def query_events_by_operation(self) -> List[Dict[str, Any]]:
        """Events count aggregated by CDC operation."""
        query = """
            SELECT operation, COUNT(*) as event_count
            FROM v_cdc_valid
            GROUP BY operation
            ORDER BY event_count DESC
        """
        return [r.asDict() for r in self.spark.sql(query).collect()]

    def query_events_by_source_table(self) -> List[Dict[str, Any]]:
        """Events count aggregated by source table and operation."""
        query = """
            SELECT source_table, operation, COUNT(*) as event_count
            FROM v_cdc_valid
            GROUP BY source_table, operation
            ORDER BY source_table, operation
        """
        return [r.asDict() for r in self.spark.sql(query).collect()]

    def query_sequence_range_by_batch(self) -> List[Dict[str, Any]]:
        """Sequence boundaries (min/max) per batch."""
        query = """
            SELECT
                batch_id,
                MIN(source_sequence) as min_sequence,
                MAX(source_sequence) as max_sequence,
                COUNT(*) as event_count
            FROM v_cdc_valid
            GROUP BY batch_id
            ORDER BY batch_id ASC
        """
        return [r.asDict() for r in self.spark.sql(query).collect()]

    def query_update_frequency_per_entity(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Entities with the highest number of update operations."""
        query = f"""
            SELECT source_table, business_key, COUNT(*) as update_count
            FROM v_cdc_valid
            WHERE operation = 'UPDATE'
            GROUP BY source_table, business_key
            ORDER BY update_count DESC
            LIMIT {limit}
        """
        return [r.asDict() for r in self.spark.sql(query).collect()]

    def query_late_events_by_table(self) -> List[Dict[str, Any]]:
        """Late-arriving events grouped by table and batch."""
        query = """
            SELECT source_table, batch_id, COUNT(*) as late_event_count,
                   MIN(source_sequence) as min_late_seq,
                   MAX(source_sequence) as max_late_seq
            FROM v_cdc_late
            GROUP BY source_table, batch_id
            ORDER BY source_table, batch_id
        """
        return [r.asDict() for r in self.spark.sql(query).collect()]

    def query_invalid_events_by_reason(self) -> List[Dict[str, Any]]:
        """Quarantined events grouped by rejection reason."""
        query = """
            SELECT rejection_reason, COUNT(*) as rejected_count
            FROM v_cdc_quarantine
            GROUP BY rejection_reason
            ORDER BY rejected_count DESC
        """
        return [r.asDict() for r in self.spark.sql(query).collect()]

    def run_all_diagnostics(self) -> Dict[str, Any]:
        """Execute all diagnostics and return structured dictionary."""
        self.register_views()
        return {
            "events_by_operation": self.query_events_by_operation(),
            "events_by_source_table": self.query_events_by_source_table(),
            "sequence_range_by_batch": self.query_sequence_range_by_batch(),
            "update_frequency_per_entity": self.query_update_frequency_per_entity(),
            "late_events_by_table": self.query_late_events_by_table(),
            "invalid_events_by_reason": self.query_invalid_events_by_reason(),
        }
