"""End-to-end Delta CDC Application pipeline with atomic batch checkpointing and crash recovery."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.cdc.deduplication import deduplicate_cdc_events
from src.cdc.discovery import DiscoveredBatch, discover_batches
from src.cdc.ordering import order_cdc_events
from src.cdc.validation import validate_cdc_dataframe
from src.config.settings import (
    DEFAULT_AUDIT_DIR,
    DEFAULT_CDC_DATA_DIR,
    DEFAULT_CHECKPOINT_DIR,
    DEFAULT_CURRENT_DIR,
    DEFAULT_QUARANTINE_DIR,
    DEFAULT_SAMPLE_DATA_DIR,
    SUPPORTED_TABLES,
)
from src.delta.audit import (
    get_recorded_ledger_event_ids,
    upsert_applied_events,
)
from src.delta.current_state import (
    initialize_delta_current_state,
    load_current_table,
)
from src.delta.merge import (
    MergeExecutionMetrics,
    classify_events_and_apply_merge,
)
from src.delta.metrics import record_batch_apply_metric
from src.delta.payload_parser import parse_cdc_payloads
from src.delta.reconciliation import reconcile_current_state
from src.schemas.cdc_schema import CDC_SPARK_SCHEMA
from src.state.checkpoint import CheckpointManager
from src.utils.spark import get_spark_session


@dataclass
class BatchApplySummary:
    batch_id: int
    tables: Dict[str, MergeExecutionMetrics] = field(default_factory=dict)
    total_valid_unique_events: int = 0
    total_applied_inserts: int = 0
    total_applied_updates: int = 0
    total_applied_deletes: int = 0
    total_superseded_events: int = 0
    total_stale_noops: int = 0
    total_already_applied: int = 0
    total_recovered_events: int = 0


@dataclass
class PipelineApplyResult:
    batches_processed: List[BatchApplySummary] = field(default_factory=list)
    skipped_batch_ids: List[int] = field(default_factory=list)
    checkpoint_state: Dict[str, int] = field(default_factory=dict)


class DeltaCDCApplier:
    """Orchestrates ingestion of validated CDC change batches into Delta current-state tables."""

    def __init__(
        self,
        spark: Optional[SparkSession] = None,
        cdc_dir: Optional[Path] = None,
        delta_dir: Optional[Path] = None,
        audit_dir: Optional[Path] = None,
        checkpoint_dir: Optional[Path] = None,
        quarantine_dir: Optional[Path] = None,
        snapshot_dir: Optional[Path] = None,
    ):
        self.spark = spark or get_spark_session()
        self.cdc_dir = cdc_dir or DEFAULT_CDC_DATA_DIR
        self.delta_dir = delta_dir or DEFAULT_CURRENT_DIR
        self.audit_dir = audit_dir or DEFAULT_AUDIT_DIR
        self.checkpoint_dir = checkpoint_dir or DEFAULT_CHECKPOINT_DIR
        self.quarantine_dir = quarantine_dir or DEFAULT_QUARANTINE_DIR
        self.snapshot_dir = snapshot_dir or DEFAULT_SAMPLE_DATA_DIR

        self.checkpoint_mgr = CheckpointManager(self.checkpoint_dir)

    def ensure_current_state_initialized(self) -> None:
        """Initialize Delta current-state tables from snapshot if not already present."""
        initialize_delta_current_state(
            spark=self.spark,
            snapshot_dir=self.snapshot_dir,
            delta_dir=self.delta_dir,
            force_overwrite=False,
        )

    def apply_batch(
        self,
        batch: DiscoveredBatch,
        simulate_failure_before_ledger: bool = False,
        simulate_failure_before_checkpoint: bool = False,
    ) -> BatchApplySummary:
        """Process and apply a single CDC batch into Delta current-state tables.

        CRITICAL IDEMPOTENCY & CRASH RECOVERY ORDER:
        1. Parse and validate events for all tables.
        2. Apply Delta MERGE to target current-state tables.
        3. Upsert audit ledger in delta/audit/applied_events.
        4. Persist batch apply metrics.
        5. Atomically commit the global batch checkpoint.
        """
        batch_id = batch.batch_id
        summary = BatchApplySummary(batch_id=batch_id)

        # Existing ledger event IDs for recovery tracking
        existing_ledger_ids = get_recorded_ledger_event_ids(self.spark, self.audit_dir)

        staged_audit_dfs: List[DataFrame] = []
        staged_hwm_updates: Dict[str, int] = {}
        staged_table_metrics: Dict[str, MergeExecutionMetrics] = {}
        staged_table_stats: Dict[str, Tuple[int, int, int, int]] = {}

        for table in sorted(SUPPORTED_TABLES):
            current_hwm = self.checkpoint_mgr.get_high_water_mark(table)
            file_path = batch.table_files.get(table)

            if not file_path or not file_path.exists():
                staged_hwm_updates[table] = current_hwm
                continue

            # 1. Read JSONL with explicit schema
            raw_df = self.spark.read.schema(CDC_SPARK_SCHEMA).json(str(file_path)).cache()
            if raw_df.count() == 0:
                staged_hwm_updates[table] = current_hwm
                continue

            # 2. Validation & Quarantine
            valid_df, quarantine_df = validate_cdc_dataframe(raw_df)
            if quarantine_df.count() > 0:
                q_out = self.quarantine_dir / f"batch_id={batch_id:06d}" / f"{table}.parquet"
                q_out.parent.mkdir(parents=True, exist_ok=True)
                quarantine_df.write.mode("append").parquet(str(q_out))

            # 3. Deterministic Deduplication
            deduped_df, _ = deduplicate_cdc_events(valid_df)

            # 4. In-batch ordering
            ordered_df = order_cdc_events(deduped_df)

            # 5. Table-specific payload parsing
            parsed_df = parse_cdc_payloads(ordered_df, table)

            # Compute sequence bounds of incoming batch
            seq_bounds = ordered_df.select(
                F.min("source_sequence").alias("min_seq"),
                F.max("source_sequence").alias("max_seq"),
            ).collect()[0]
            min_seq = seq_bounds["min_seq"] or 0
            max_seq = seq_bounds["max_seq"] or 0

            # 6. Classify events and apply Delta MERGE
            audit_df, metrics = classify_events_and_apply_merge(
                spark=self.spark,
                source_table=table,
                parsed_batch_df=parsed_df,
                batch_id=batch_id,
                delta_dir=self.delta_dir,
                existing_ledger_event_ids=existing_ledger_ids,
            )

            staged_audit_dfs.append(audit_df)
            staged_table_metrics[table] = metrics
            summary.tables[table] = metrics

            summary.total_valid_unique_events += metrics.total_batch_events
            summary.total_applied_inserts += metrics.applied_inserts
            summary.total_applied_updates += metrics.applied_updates
            summary.total_applied_deletes += metrics.applied_deletes
            summary.total_superseded_events += metrics.superseded_events
            summary.total_stale_noops += metrics.stale_noops
            summary.total_already_applied += metrics.already_applied
            summary.total_recovered_events += metrics.recovered_events

            # Query updated target state counts
            tgt_df = load_current_table(self.spark, table, self.delta_dir)
            active_cnt = tgt_df.filter(F.col("_is_deleted") == False).count()  # noqa: E712
            deleted_cnt = tgt_df.filter(F.col("_is_deleted") == True).count()  # noqa: E712

            staged_table_stats[table] = (active_cnt, deleted_cnt, min_seq, max_seq)
            staged_hwm_updates[table] = max(current_hwm, max_seq)

        # SIMULATED CRASH POINT 1: Target committed, but ledger not written
        if simulate_failure_before_ledger:
            raise RuntimeError(
                f"Simulated crash triggered after target MERGE but BEFORE audit ledger for batch {batch_id}!"
            )

        # 7. Merge into Applied Event Ledger
        if staged_audit_dfs:
            combined_audit = staged_audit_dfs[0]
            for extra_df in staged_audit_dfs[1:]:
                combined_audit = combined_audit.unionByName(extra_df)

            upsert_applied_events(self.spark, combined_audit, self.audit_dir)

        # 8. Record Batch Apply Metrics
        for table, metrics in staged_table_metrics.items():
            active_cnt, deleted_cnt, min_seq, max_seq = staged_table_stats[table]
            record_batch_apply_metric(
                spark=self.spark,
                batch_id=batch_id,
                source_table=table,
                metrics=metrics,
                current_active_rows=active_cnt,
                current_deleted_rows=deleted_cnt,
                min_sequence=min_seq,
                max_sequence=max_seq,
                audit_dir=self.audit_dir,
            )

        # SIMULATED CRASH POINT 2: Ledger written, but checkpoint not advanced
        if simulate_failure_before_checkpoint:
            raise RuntimeError(
                f"Simulated crash triggered BEFORE atomic checkpoint commit for batch {batch_id}!"
            )

        # 9. ATOMIC GLOBAL BATCH CHECKPOINT ADVANCEMENT
        self.checkpoint_mgr.commit_batch(
            batch_id=batch_id,
            table_sequences=staged_hwm_updates,
        )

        # 10. Run reconciliation invariants
        reconcile_current_state(self.spark, self.delta_dir, self.audit_dir)

        return summary

    def run_apply_pipeline(self) -> PipelineApplyResult:
        """Discover pending CDC batches and apply them sequentially into Delta current state."""
        self.ensure_current_state_initialized()

        batches = discover_batches(self.cdc_dir)
        completed_batches = set(self.checkpoint_mgr.get_completed_batches())

        result = PipelineApplyResult()

        for batch in batches:
            if batch.batch_id in completed_batches:
                result.skipped_batch_ids.append(batch.batch_id)
                continue

            summary = self.apply_batch(batch)
            result.batches_processed.append(summary)
            completed_batches.add(batch.batch_id)

        result.checkpoint_state = {
            t: self.checkpoint_mgr.get_high_water_mark(t) for t in sorted(SUPPORTED_TABLES)
        }
        return result
