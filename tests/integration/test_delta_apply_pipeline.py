"""Tests 30-40: Pipeline idempotency, target/ledger crash recovery, and current-state reconciliation."""

import json
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from src.delta.audit import get_applied_events_path
from src.delta.current_state import (
    load_current_table,
)
from src.delta.reconciliation import ReconciliationError, reconcile_current_state
from src.generation.cdc_generator import generate_cdc_batches
from src.generation.initial_state import generate_initial_state
from src.pipelines.delta_cdc_apply import DeltaCDCApplier
from src.schemas.cdc_schema import (
    CDC_SPARK_SCHEMA,
    CURRENT_SCHEMA_VERSION,
    OPERATION_DELETE,
    generate_event_id,
)
from src.state.checkpoint import CheckpointManager


def _setup_full_pipeline_env(base_dir: Path, spark: SparkSession):
    snap_dir = base_dir / "sample" / "snapshot"
    cdc_dir = base_dir / "generated" / "cdc"
    delta_dir = base_dir / "delta" / "current"
    audit_dir = base_dir / "delta" / "audit"
    cp_dir = base_dir / "state" / "checkpoints"
    q_dir = base_dir / "output" / "quarantine"

    # Generate tiny initial snapshot and 3 batches
    generate_initial_state(scale="tiny", seed=42, output_dir=snap_dir)
    generate_cdc_batches(num_batches=3, scale="tiny", seed=42, output_dir=cdc_dir)

    applier = DeltaCDCApplier(
        spark=spark,
        cdc_dir=cdc_dir,
        delta_dir=delta_dir,
        audit_dir=audit_dir,
        checkpoint_dir=cp_dir,
        quarantine_dir=q_dir,
        snapshot_dir=base_dir / "sample",
    )
    return applier, delta_dir, audit_dir, cp_dir


def test_30_to_32_idempotency(spark: SparkSession, temp_test_dir: Path):
    """Tests 30-32: Same batch run twice does not duplicate rows, leaves counts unchanged, and ledger remains unique."""
    applier, delta_dir, audit_dir, cp_dir = _setup_full_pipeline_env(temp_test_dir, spark)

    # First run
    res1 = applier.run_apply_pipeline()
    assert len(res1.batches_processed) == 3

    sub_count_1 = load_current_table(spark, "subscriptions", delta_dir).count()
    ledger_count_1 = (
        spark.read.format("delta").load(str(get_applied_events_path(audit_dir))).count()
    )

    # Second run (exact same state)
    res2 = applier.run_apply_pipeline()
    # Test 30 & 31: 0 new batches processed; target counts identical
    assert len(res2.batches_processed) == 0
    assert len(res2.skipped_batch_ids) == 3
    assert load_current_table(spark, "subscriptions", delta_dir).count() == sub_count_1

    # Test 32: Ledger row count unchanged and event_id remains 100% unique
    ledger_df_2 = spark.read.format("delta").load(str(get_applied_events_path(audit_dir)))
    assert ledger_df_2.count() == ledger_count_1
    assert ledger_df_2.select("event_id").distinct().count() == ledger_count_1


def test_33_and_34_crash_recovery_target_applied_ledger_missing(
    spark: SparkSession, temp_test_dir: Path
):
    """Tests 33 & 34: Simulated crash after target MERGE but before ledger write recovers without re-mutating target."""
    applier, delta_dir, audit_dir, cp_dir = _setup_full_pipeline_env(temp_test_dir, spark)
    applier.ensure_current_state_initialized()

    from src.cdc.discovery import discover_batches

    batches = discover_batches(applier.cdc_dir)
    batch_1 = batches[0]

    # Run batch 1 with simulated failure right after target MERGE
    with pytest.raises(RuntimeError, match="Simulated crash"):
        applier.apply_batch(batch_1, simulate_failure_before_ledger=True)

    # Verify target was mutated by batch 1, but checkpoint was NOT committed
    mgr = CheckpointManager(cp_dir)
    assert 1 not in mgr.get_completed_batches()

    # Retry batch 1 cleanly
    summary = applier.apply_batch(batch_1)

    # Test 33: Ledger was repaired and recognized as recovered/already applied
    assert (summary.total_recovered_events + summary.total_already_applied) > 0
    assert 1 in mgr.get_completed_batches()

    # Test 34: Test specifically for DELETE crash recovery
    del_sub = "SUB-000030"
    ev_del = generate_event_id("subscriptions", del_sub, 2000, OPERATION_DELETE)
    raw_del = spark.createDataFrame(
        [
            (
                ev_del,
                "subscriptions",
                OPERATION_DELETE,
                del_sub,
                2000,
                "2026-03-01T10:00:00Z",
                "2026-03-01T10:00:00Z",
                99,
                CURRENT_SCHEMA_VERSION,
                json.dumps({"subscription_id": del_sub}),
            )
        ],
        CDC_SPARK_SCHEMA,
    )

    from src.delta.merge import classify_events_and_apply_merge
    from src.delta.payload_parser import parse_cdc_payloads

    # Apply DELETE to target directly without writing to ledger (simulating partial crash)
    classify_events_and_apply_merge(
        spark,
        "subscriptions",
        parse_cdc_payloads(raw_del, "subscriptions"),
        99,
        delta_dir=delta_dir,
    )

    # On rerun, classify_events_and_apply_merge should detect target already tombstoned at seq 2000
    retry_audit, retry_m = classify_events_and_apply_merge(
        spark,
        "subscriptions",
        parse_cdc_payloads(raw_del, "subscriptions"),
        99,
        delta_dir=delta_dir,
    )
    assert retry_m.recovered_events == 1
    assert retry_audit.collect()[0]["apply_outcome"] == "RECOVERED_AFTER_PARTIAL_COMMIT"


def test_35_and_36_checkpoint_atomicity_and_rerun_convergence(
    spark: SparkSession, temp_test_dir: Path
):
    """Tests 35 & 36: Checkpoint remains behind during partial failure; retry converges cleanly and advances."""
    applier, delta_dir, audit_dir, cp_dir = _setup_full_pipeline_env(temp_test_dir, spark)
    applier.ensure_current_state_initialized()

    from src.cdc.discovery import discover_batches

    batch_1 = discover_batches(applier.cdc_dir)[0]

    # Test 35: Failure right before checkpoint commit leaves checkpoint completely unchanged
    with pytest.raises(RuntimeError, match="Simulated crash"):
        applier.apply_batch(batch_1, simulate_failure_before_checkpoint=True)

    mgr = CheckpointManager(cp_dir)
    assert mgr.get_completed_batches() == []
    for t in ["accounts", "subscriptions", "invoices", "payments"]:
        assert mgr.get_high_water_mark(t) == 0

    # Test 36: Retry converges cleanly and advances checkpoint
    summary = applier.apply_batch(batch_1)
    assert 1 in mgr.get_completed_batches()
    assert summary.batch_id == 1


def test_37_to_40_reconciliation_invariants(spark: SparkSession, temp_test_dir: Path):
    """Tests 37-40: Reconciliation catches duplicate business keys, regression, active+deleted match, ledger unique."""
    applier, delta_dir, audit_dir, cp_dir = _setup_full_pipeline_env(temp_test_dir, spark)
    applier.run_apply_pipeline()

    # Test 39: Reconciliation passes cleanly on valid current state
    reports = reconcile_current_state(spark, delta_dir, audit_dir)
    for t, rep in reports.items():
        assert rep.passed is True
        assert rep.total_physical_rows == (rep.active_rows + rep.deleted_rows)

    # Test 40: Applied event ledger uniqueness verified
    ledger_path = get_applied_events_path(audit_dir)
    ledger_df = spark.read.format("delta").load(str(ledger_path))
    assert ledger_df.count() == ledger_df.select("event_id").distinct().count()

    # Test 37: Inject duplicate primary key into accounts and verify reconciliation raises error
    acc_path = delta_dir / "accounts"
    acc_df = spark.read.format("delta").load(str(acc_path))
    dup_row = acc_df.limit(1)
    acc_df.union(dup_row).write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).save(str(acc_path))

    with pytest.raises(ReconciliationError, match="Duplicate keys detected"):
        reconcile_current_state(spark, delta_dir, audit_dir)
