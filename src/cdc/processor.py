"""Core incremental CDC batch processor pipeline."""

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.cdc.deduplication import deduplicate_cdc_events
from src.cdc.discovery import DiscoveredBatch, discover_batches
from src.cdc.ordering import order_cdc_events
from src.cdc.validation import validate_cdc_dataframe
from src.config.settings import (
    DEFAULT_CDC_DATA_DIR,
    DEFAULT_CHECKPOINT_DIR,
    DEFAULT_OUTPUT_DIR,
    SUPPORTED_TABLES,
)
from src.schemas.cdc_schema import CDC_SPARK_SCHEMA
from src.state.checkpoint import CheckpointManager
from src.utils.spark import get_spark_session


@dataclass
class TableProcessingMetrics:
    """Metrics captured per source table during incremental batch execution."""

    source_table: str
    raw_event_count: int
    valid_event_count: int
    invalid_event_count: int
    duplicate_event_count: int
    valid_unique_count: int
    late_event_count: int
    minimum_sequence: int
    maximum_sequence: int

    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class BatchProcessingResult:
    """Full execution summary for a processed CDC batch."""

    batch_id: int
    status: str
    processed_at: str
    table_metrics: Dict[str, TableProcessingMetrics]
    total_raw_events: int
    total_valid_unique: int
    total_duplicates: int
    total_invalid: int
    total_late_events: int


class CDCProcessor:
    """End-to-end incremental CDC batch processor."""

    def __init__(
        self,
        spark: Optional[SparkSession] = None,
        cdc_dir: Optional[Path] = None,
        checkpoint_dir: Optional[Path] = None,
        output_dir: Optional[Path] = None,
    ):
        self.spark = spark or get_spark_session()
        self.cdc_dir = cdc_dir or DEFAULT_CDC_DATA_DIR
        self.checkpoint_dir = checkpoint_dir or DEFAULT_CHECKPOINT_DIR
        self.checkpoint_mgr = CheckpointManager(self.checkpoint_dir)

        self.output_dir = output_dir or DEFAULT_OUTPUT_DIR
        self.valid_dir = self.output_dir / "valid_events"
        self.quarantine_dir = self.output_dir / "quarantine"
        self.late_dir = self.output_dir / "late_events"
        self.metrics_dir = self.output_dir / "metrics"

        for p in [self.valid_dir, self.quarantine_dir, self.late_dir, self.metrics_dir]:
            p.mkdir(parents=True, exist_ok=True)

    def process_all_pending_batches(self) -> List[BatchProcessingResult]:
        """Discover and process all pending batches in strict numeric order."""
        batches = discover_batches(self.cdc_dir)
        results: List[BatchProcessingResult] = []

        for batch in batches:
            if self.checkpoint_mgr.is_batch_completed_for_all(batch.batch_id):
                continue
            res = self.process_batch(batch)
            results.append(res)

        return results

    def process_batch(
        self,
        batch: DiscoveredBatch,
        simulate_failure: bool = False,
    ) -> BatchProcessingResult:
        """Process a single discovered CDC batch.

        Guarantees:
        1. Explicit CDC schema application (no inferSchema)
        2. Event validation & quarantine isolation
        3. Deterministic ROW_NUMBER() deduplication
        4. Strict source_sequence ordering
        5. Late-event classification against persistent high-water mark
        6. Invariant metrics reconciliation:
           raw_event_count == valid_unique_count + duplicate_event_count + invalid_event_count
        7. Atomic checkpoint update ONLY after complete success
        """
        batch_id = batch.batch_id
        batch_tag = f"batch_id={batch_id:06d}"
        proc_time = datetime.now(timezone.utc).isoformat()
        table_metrics_map: Dict[str, TableProcessingMetrics] = {}

        # Collect data for tables present in this batch
        all_table_names = set(SUPPORTED_TABLES)

        # Temporary in-memory staging for table-level updates before checkpoint commit
        staged_checkpoint_updates: Dict[str, int] = {}
        staged_outputs: List[Tuple[str, DataFrame, Path]] = []

        for table in sorted(all_table_names):
            current_hwm = self.checkpoint_mgr.get_high_water_mark(table)
            file_path = batch.table_files.get(table)

            if not file_path or not file_path.exists():
                table_metrics_map[table] = TableProcessingMetrics(
                    source_table=table,
                    raw_event_count=0,
                    valid_event_count=0,
                    invalid_event_count=0,
                    duplicate_event_count=0,
                    valid_unique_count=0,
                    late_event_count=0,
                    minimum_sequence=0,
                    maximum_sequence=0,
                )
                staged_checkpoint_updates[table] = current_hwm
                continue

            # 1. Read JSONL using explicit schema
            raw_df = self.spark.read.schema(CDC_SPARK_SCHEMA).json(str(file_path)).cache()
            raw_count = raw_df.count()

            # 2. Validation & Quarantine
            valid_df, quarantine_df = validate_cdc_dataframe(raw_df)
            valid_count = valid_df.count()
            invalid_count = quarantine_df.count()

            # 3. Deterministic Deduplication
            deduped_valid_df, duplicates_df = deduplicate_cdc_events(valid_df)
            valid_unique_count = deduped_valid_df.count()
            duplicate_count = duplicates_df.count()

            # Reconciliation Check
            if raw_count != (valid_unique_count + duplicate_count + invalid_count):
                raise RuntimeError(
                    f"Metrics reconciliation failed for table '{table}' in batch {batch_id}: "
                    f"raw({raw_count}) != valid_unique({valid_unique_count}) + "
                    f"duplicates({duplicate_count}) + invalid({invalid_count})"
                )

            # 4. Source-Sequence Ordering
            ordered_df = order_cdc_events(deduped_valid_df).cache()

            # 5. Late-Event Classification against Table High-Water Mark
            # Late event: source_sequence <= current_hwm
            if current_hwm > 0:
                late_events_df = ordered_df.filter(F.col("source_sequence") <= current_hwm).cache()
            else:
                late_events_df = self.spark.createDataFrame([], CDC_SPARK_SCHEMA).cache()

            late_count = late_events_df.count()

            # Calculate sequence range on valid unique records
            seq_stats = ordered_df.select(
                F.min("source_sequence").alias("min_seq"),
                F.max("source_sequence").alias("max_seq"),
            ).collect()

            min_seq = seq_stats[0]["min_seq"] or 0
            max_seq = seq_stats[0]["max_seq"] or 0

            # Record table metrics
            table_metrics_map[table] = TableProcessingMetrics(
                source_table=table,
                raw_event_count=raw_count,
                valid_event_count=valid_count,
                invalid_event_count=invalid_count,
                duplicate_event_count=duplicate_count,
                valid_unique_count=valid_unique_count,
                late_event_count=late_count,
                minimum_sequence=min_seq,
                maximum_sequence=max_seq,
            )

            # Stage outputs
            valid_out_path = self.valid_dir / batch_tag / f"{table}.jsonl"
            quarantine_out_path = self.quarantine_dir / batch_tag / f"{table}.jsonl"
            late_out_path = self.late_dir / batch_tag / f"{table}.jsonl"

            staged_outputs.append(("valid", ordered_df, valid_out_path))
            if invalid_count > 0:
                staged_outputs.append(("quarantine", quarantine_df, quarantine_out_path))
            if late_count > 0:
                staged_outputs.append(("late", late_events_df, late_out_path))

            # Stage checkpoint progression
            next_hwm = max(current_hwm, max_seq)
            staged_checkpoint_updates[table] = next_hwm

        # Write staged outputs to storage
        for out_type, out_df, out_path in staged_outputs:
            out_path.parent.mkdir(parents=True, exist_ok=True)
            # Write JSONL
            rows = [r.asDict() for r in out_df.collect()]
            with open(out_path, "w", encoding="utf-8") as f:
                for row in rows:
                    f.write(json.dumps(row) + "\n")

        # Compile and save batch metrics
        total_raw = sum(m.raw_event_count for m in table_metrics_map.values())
        total_valid_u = sum(m.valid_unique_count for m in table_metrics_map.values())
        total_dup = sum(m.duplicate_event_count for m in table_metrics_map.values())
        total_inv = sum(m.invalid_event_count for m in table_metrics_map.values())
        total_late = sum(m.late_event_count for m in table_metrics_map.values())

        batch_result = BatchProcessingResult(
            batch_id=batch_id,
            status="SUCCESS",
            processed_at=proc_time,
            table_metrics=table_metrics_map,
            total_raw_events=total_raw,
            total_valid_unique=total_valid_u,
            total_duplicates=total_dup,
            total_invalid=total_inv,
            total_late_events=total_late,
        )

        metrics_file = self.metrics_dir / batch_tag / "metrics.json"
        metrics_file.parent.mkdir(parents=True, exist_ok=True)
        with open(metrics_file, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "batch_id": batch_id,
                    "status": "SUCCESS",
                    "processed_at": proc_time,
                    "totals": {
                        "raw_events": total_raw,
                        "valid_unique": total_valid_u,
                        "duplicates": total_dup,
                        "invalid": total_inv,
                        "late_events": total_late,
                    },
                    "tables": {t: m.to_dict() for t, m in table_metrics_map.items()},
                },
                f,
                indent=2,
            )

        # SIMULATE FAILURE POINT:
        # If simulated failure is requested, raise exception right before checkpoint commit
        if simulate_failure:
            raise RuntimeError(
                f"Simulated pipeline processing failure triggered for batch {batch_id} before checkpoint commit!"
            )

        # 6. ATOMIC CHECKPOINT ADVANCEMENT (ONLY ON COMPLETE SUCCESS)
        for table, new_hwm in staged_checkpoint_updates.items():
            self.checkpoint_mgr.advance_high_water_mark(
                table=table,
                sequence=new_hwm,
                batch_id=batch_id,
            )

        return batch_result
