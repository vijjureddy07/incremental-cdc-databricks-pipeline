"""Current-state invariants verification and data quality reconciliation."""

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from src.config.settings import SUPPORTED_TABLES
from src.delta.audit import get_applied_events_path
from src.delta.current_state import get_current_table_path
from src.delta.schemas import TARGET_PRIMARY_KEYS


class ReconciliationError(RuntimeError):
    """Raised when current-state table invariants or audit ledger consistency checks fail."""

    pass


@dataclass
class TableReconciliationReport:
    table_name: str
    total_physical_rows: int
    active_rows: int
    deleted_rows: int
    min_source_sequence: int
    max_source_sequence: int
    passed: bool


def reconcile_current_state(
    spark: SparkSession,
    delta_dir: Optional[Path] = None,
    audit_dir: Optional[Path] = None,
) -> Dict[str, TableReconciliationReport]:
    """Execute complete invariant reconciliation across all Delta current-state tables and the audit ledger.

    MANDATORY INVARIANTS:
    1. Exactly one physical row per primary key (no duplicate keys).
    2. Primary key is non-null for all physical rows.
    3. _last_source_sequence is strictly positive (> 0) for all rows.
    4. active_rows + deleted_rows == total_physical_rows.
    5. Monetary attributes must be strictly typed as DecimalType.
    6. Applied event IDs in delta/audit/applied_events must be 100% unique.
    """
    reports: Dict[str, TableReconciliationReport] = {}

    for table in sorted(SUPPORTED_TABLES):
        table_path = get_current_table_path(table, delta_dir)
        if not table_path.exists():
            continue

        df = spark.read.format("delta").load(str(table_path))
        pk_col = TARGET_PRIMARY_KEYS[table]

        total_rows = df.count()

        # Invariant 1 & 2: Primary Key non-null & unique
        null_pks = df.filter(F.col(pk_col).isNull()).count()
        if null_pks > 0:
            raise ReconciliationError(
                f"Reconciliation failure in table '{table}': found {null_pks} null primary keys!"
            )

        distinct_pks = df.select(pk_col).distinct().count()
        if distinct_pks != total_rows:
            raise ReconciliationError(
                f"Reconciliation failure in table '{table}': total rows ({total_rows}) "
                f"!= distinct primary keys ({distinct_pks}). Duplicate keys detected!"
            )

        # Invariant 3: Sequences strictly positive
        invalid_seqs = df.filter(
            (F.col("_last_source_sequence") <= 0) | F.col("_last_source_sequence").isNull()
        ).count()
        if invalid_seqs > 0:
            raise ReconciliationError(
                f"Reconciliation failure in table '{table}': found {invalid_seqs} rows with sequence <= 0!"
            )

        # Invariant 4: Active + Deleted == Total
        active_count = df.filter(F.col("_is_deleted") == False).count()  # noqa: E712
        deleted_count = df.filter(F.col("_is_deleted") == True).count()  # noqa: E712
        if (active_count + deleted_count) != total_rows:
            raise ReconciliationError(
                f"Reconciliation failure in table '{table}': active ({active_count}) + "
                f"deleted ({deleted_count}) != total rows ({total_rows})!"
            )

        # Invariant 5: Decimal monetary types
        schema_dict = {f.name: f.dataType for f in df.schema.fields}
        if table == "subscriptions":
            if not isinstance(schema_dict.get("monthly_amount"), DecimalType):
                raise ReconciliationError(
                    f"Subscriptions monthly_amount is {schema_dict.get('monthly_amount')}, not DecimalType!"
                )
        elif table in ("invoices", "payments"):
            if not isinstance(schema_dict.get("amount"), DecimalType):
                raise ReconciliationError(
                    f"{table.capitalize()} amount is {schema_dict.get('amount')}, not DecimalType!"
                )

        # Compute sequence ranges
        seq_stats = df.select(
            F.min("_last_source_sequence").alias("min_seq"),
            F.max("_last_source_sequence").alias("max_seq"),
        ).collect()[0]

        reports[table] = TableReconciliationReport(
            table_name=table,
            total_physical_rows=total_rows,
            active_rows=active_count,
            deleted_rows=deleted_count,
            min_source_sequence=seq_stats["min_seq"] or 0,
            max_source_sequence=seq_stats["max_seq"] or 0,
            passed=True,
        )

    # Invariant 6: Applied event ledger uniqueness
    ledger_path = get_applied_events_path(audit_dir)
    if ledger_path.exists():
        ledger_df = spark.read.format("delta").load(str(ledger_path))
        total_ledger = ledger_df.count()
        distinct_event_ids = ledger_df.select("event_id").distinct().count()
        if distinct_event_ids != total_ledger:
            raise ReconciliationError(
                f"Reconciliation failure in applied_events ledger: total rows ({total_ledger}) "
                f"!= distinct event_ids ({distinct_event_ids}). Duplicate audit events detected!"
            )

    return reports
