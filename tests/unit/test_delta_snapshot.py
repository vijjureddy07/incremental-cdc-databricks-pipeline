"""Tests 6-12: Delta snapshot initialization, Decimal types, and active view helpers."""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType

from src.delta.current_state import (
    initialize_delta_current_state,
    load_active_table,
    load_current_table,
)
from src.generation.initial_state import generate_initial_state


def _ensure_test_snapshot(base_dir: Path) -> Path:
    snap_dir = base_dir / "sample" / "snapshot"
    generate_initial_state(scale="tiny", seed=42, output_dir=snap_dir)
    return base_dir / "sample"


def test_06_to_09_all_snapshots_initialize_delta(spark: SparkSession, temp_test_dir: Path):
    """Tests 6-9: accounts, subscriptions, invoices, and payments snapshot initialize Delta current-state."""
    sample_dir = _ensure_test_snapshot(temp_test_dir)
    delta_dir = temp_test_dir / "delta" / "current"

    counts = initialize_delta_current_state(
        spark=spark,
        snapshot_dir=sample_dir,
        delta_dir=delta_dir,
        force_overwrite=True,
    )

    # Test 6: accounts initialized
    assert counts["accounts"] == 50
    acc_df = load_current_table(spark, "accounts", delta_dir)
    assert acc_df.count() == 50

    # Test 7: subscriptions initialized
    assert counts["subscriptions"] == 100
    sub_df = load_current_table(spark, "subscriptions", delta_dir)
    assert sub_df.count() == 100

    # Test 8: invoices initialized
    assert counts["invoices"] == 250
    inv_df = load_current_table(spark, "invoices", delta_dir)
    assert inv_df.count() == 250

    # Test 9: payments initialized
    assert counts["payments"] == 200
    pay_df = load_current_table(spark, "payments", delta_dir)
    assert pay_df.count() == 200


def test_10_monetary_columns_are_decimal(spark: SparkSession, temp_test_dir: Path):
    """Test 10: Monetary attributes (monthly_amount, invoice amount, payment amount) are DecimalType."""
    sample_dir = _ensure_test_snapshot(temp_test_dir)
    delta_dir = temp_test_dir / "delta" / "current"
    initialize_delta_current_state(spark=spark, snapshot_dir=sample_dir, delta_dir=delta_dir)

    sub_schema = {
        f.name: f.dataType
        for f in load_current_table(spark, "subscriptions", delta_dir).schema.fields
    }
    inv_schema = {
        f.name: f.dataType for f in load_current_table(spark, "invoices", delta_dir).schema.fields
    }
    pay_schema = {
        f.name: f.dataType for f in load_current_table(spark, "payments", delta_dir).schema.fields
    }

    assert isinstance(sub_schema["monthly_amount"], DecimalType)
    assert isinstance(inv_schema["amount"], DecimalType)
    assert isinstance(pay_schema["amount"], DecimalType)


def test_11_one_physical_row_per_business_key(spark: SparkSession, temp_test_dir: Path):
    """Test 11: Each initial current-state table contains exactly one physical row per business key."""
    sample_dir = _ensure_test_snapshot(temp_test_dir)
    delta_dir = temp_test_dir / "delta" / "current"
    initialize_delta_current_state(spark=spark, snapshot_dir=sample_dir, delta_dir=delta_dir)

    for table, pk in [
        ("accounts", "account_id"),
        ("subscriptions", "subscription_id"),
        ("invoices", "invoice_id"),
        ("payments", "payment_id"),
    ]:
        df = load_current_table(spark, table, delta_dir)
        total = df.count()
        distinct_pks = df.select(pk).distinct().count()
        assert total == distinct_pks, f"Table {table} has duplicate keys!"


def test_12_all_initial_rows_have_is_deleted_false(spark: SparkSession, temp_test_dir: Path):
    """Test 12: All initial snapshot rows have _is_deleted=false and _last_event_id='SNAPSHOT_INIT'."""
    sample_dir = _ensure_test_snapshot(temp_test_dir)
    delta_dir = temp_test_dir / "delta" / "current"
    initialize_delta_current_state(spark=spark, snapshot_dir=sample_dir, delta_dir=delta_dir)

    for table in ["accounts", "subscriptions", "invoices", "payments"]:
        df = load_current_table(spark, table, delta_dir)
        active_df = load_active_table(spark, table, delta_dir)
        assert df.count() == active_df.count()

        non_initial_rows = df.filter(df["_last_event_id"] != "SNAPSHOT_INIT").count()
        assert non_initial_rows == 0
