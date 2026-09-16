"""Integration tests for end-to-end incremental CDC batch processing pipeline."""

from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from src.cdc.analysis import CDCAnalyzer
from src.cdc.discovery import discover_batches
from src.cdc.processor import CDCProcessor
from src.config.settings import DefectConfig
from src.generation.cdc_generator import generate_cdc_batches
from src.state.checkpoint import CheckpointManager


def test_20_failed_processing_does_not_advance_checkpoint(spark: SparkSession, temp_test_dir: Path):
    """Test 20: Failed processing does NOT advance checkpoint."""
    cdc_dir = temp_test_dir / "cdc_fail"
    cp_dir = temp_test_dir / "checkpoints_fail"
    out_dir = temp_test_dir / "output_fail"

    generate_cdc_batches(num_batches=1, scale="tiny", seed=10, output_dir=cdc_dir)
    discovered = discover_batches(cdc_dir)
    batch1 = discovered[0]

    processor = CDCProcessor(
        spark=spark,
        cdc_dir=cdc_dir,
        checkpoint_dir=cp_dir,
        output_dir=out_dir,
    )
    cp_mgr = CheckpointManager(cp_dir)

    # Initial checkpoint is 0
    assert cp_mgr.get_high_water_mark("accounts") == 0

    # Simulated failure right before commit
    with pytest.raises(RuntimeError) as exc_info:
        processor.process_batch(batch1, simulate_failure=True)
    assert "Simulated pipeline processing failure" in str(exc_info.value)

    # Checkpoint must remain 0
    assert cp_mgr.get_high_water_mark("accounts") == 0
    assert cp_mgr.get_high_water_mark("subscriptions") == 0


def test_21_successful_processing_advances_checkpoint(spark: SparkSession, temp_test_dir: Path):
    """Test 21: Successful processing advances checkpoint."""
    cdc_dir = temp_test_dir / "cdc_succ"
    cp_dir = temp_test_dir / "checkpoints_succ"
    out_dir = temp_test_dir / "output_succ"

    generate_cdc_batches(num_batches=1, scale="tiny", seed=10, output_dir=cdc_dir)
    discovered = discover_batches(cdc_dir)
    batch1 = discovered[0]

    processor = CDCProcessor(
        spark=spark,
        cdc_dir=cdc_dir,
        checkpoint_dir=cp_dir,
        output_dir=out_dir,
    )
    cp_mgr = CheckpointManager(cp_dir)

    res = processor.process_batch(batch1, simulate_failure=False)
    assert res.status == "SUCCESS"

    acc_hwm = cp_mgr.get_high_water_mark("accounts")
    sub_hwm = cp_mgr.get_high_water_mark("subscriptions")
    assert acc_hwm > 0
    assert sub_hwm > 0
    assert cp_mgr.load_checkpoint("accounts").last_processed_batch_id == 1


def test_22_completed_batch_rerun_processes_zero_new_batches(
    spark: SparkSession, temp_test_dir: Path
):
    """Test 22: Completed batch rerun processes zero new batches (idempotency)."""
    cdc_dir = temp_test_dir / "cdc_idemp"
    cp_dir = temp_test_dir / "checkpoints_idemp"
    out_dir = temp_test_dir / "output_idemp"

    generate_cdc_batches(num_batches=2, scale="tiny", seed=20, output_dir=cdc_dir)

    processor = CDCProcessor(
        spark=spark,
        cdc_dir=cdc_dir,
        checkpoint_dir=cp_dir,
        output_dir=out_dir,
    )

    first_run = processor.process_all_pending_batches()
    assert len(first_run) == 2

    second_run = processor.process_all_pending_batches()
    assert len(second_run) == 0


def test_16_late_event_classified(spark: SparkSession, temp_test_dir: Path):
    """Test 16: Late event classified relative to checkpoint and not silently discarded."""
    cdc_dir = temp_test_dir / "cdc_late"
    cp_dir = temp_test_dir / "checkpoints_late"
    out_dir = temp_test_dir / "output_late"

    defects = DefectConfig(late_event_rate=0.15)
    generate_cdc_batches(num_batches=3, scale="tiny", seed=33, output_dir=cdc_dir, defects=defects)

    processor = CDCProcessor(
        spark=spark,
        cdc_dir=cdc_dir,
        checkpoint_dir=cp_dir,
        output_dir=out_dir,
    )
    results = processor.process_all_pending_batches()

    total_late_events = sum(r.total_late_events for r in results)
    assert total_late_events > 0, "Expected late events in multi-batch sequence"

    late_files = list((out_dir / "late_events").glob("*/*.jsonl"))
    assert len(late_files) > 0, "Late events must be persisted to late_events/ and not discarded"


def test_23_metrics_reconcile(spark: SparkSession, temp_test_dir: Path):
    """Test 23: Event metrics reconcile: raw == valid_unique + duplicates + invalid."""
    cdc_dir = temp_test_dir / "cdc_metrics"
    cp_dir = temp_test_dir / "checkpoints_metrics"
    out_dir = temp_test_dir / "output_metrics"

    defects = DefectConfig(
        duplicate_event_rate=0.10,
        late_event_rate=0.10,
        unknown_operation_rate=0.05,
        malformed_payload_rate=0.05,
        missing_key_rate=0.05,
    )
    generate_cdc_batches(num_batches=3, scale="tiny", seed=77, output_dir=cdc_dir, defects=defects)

    processor = CDCProcessor(
        spark=spark,
        cdc_dir=cdc_dir,
        checkpoint_dir=cp_dir,
        output_dir=out_dir,
    )
    results = processor.process_all_pending_batches()
    assert len(results) == 3

    for r in results:
        for t_name, tm in r.table_metrics.items():
            assert tm.raw_event_count == (
                tm.valid_unique_count + tm.duplicate_event_count + tm.invalid_event_count
            ), f"Metrics reconciliation invariant failed for {t_name}"


def test_24_spark_sql_analysis_works(spark: SparkSession, temp_test_dir: Path):
    """Test 24: Spark SQL diagnostic analysis runs successfully on outputs."""
    cdc_dir = temp_test_dir / "cdc"
    cp_dir = temp_test_dir / "checkpoints"
    out_dir = temp_test_dir / "output"

    defects = DefectConfig(
        duplicate_event_rate=0.05,
        late_event_rate=0.05,
        unknown_operation_rate=0.05,
        malformed_payload_rate=0.05,
    )
    generate_cdc_batches(num_batches=2, scale="tiny", seed=50, output_dir=cdc_dir, defects=defects)

    processor = CDCProcessor(
        spark=spark,
        cdc_dir=cdc_dir,
        checkpoint_dir=cp_dir,
        output_dir=out_dir,
    )
    processor.process_all_pending_batches()

    analyzer = CDCAnalyzer(
        spark=spark,
        valid_dir=out_dir / "valid_events",
        quarantine_dir=out_dir / "quarantine",
        late_dir=out_dir / "late_events",
    )
    diag = analyzer.run_all_diagnostics()

    assert "events_by_operation" in diag
    assert "events_by_source_table" in diag
    assert "sequence_range_by_batch" in diag
    assert "update_frequency_per_entity" in diag
    assert "late_events_by_table" in diag
    assert "invalid_events_by_reason" in diag

    assert len(diag["events_by_operation"]) > 0
    assert len(diag["sequence_range_by_batch"]) == 2
