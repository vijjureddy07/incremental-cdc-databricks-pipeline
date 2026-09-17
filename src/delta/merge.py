"""Delta Lake MERGE logic with soft-delete tombstones, sequence collision guards, and within-batch collapsing."""

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional, Tuple

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.delta.audit import get_applied_events_path
from src.delta.current_state import get_current_table_path
from src.delta.schemas import (
    LINEAGE_FIELD_NAMES,
    OUTCOME_ALREADY_APPLIED,
    OUTCOME_APPLIED_DELETE,
    OUTCOME_APPLIED_INSERT,
    OUTCOME_APPLIED_UPDATE,
    OUTCOME_RECOVERED_AFTER_PARTIAL_COMMIT,
    OUTCOME_STALE_NOOP,
    OUTCOME_SUPERSEDED_WITHIN_BATCH,
    TARGET_PRIMARY_KEYS,
)


@dataclass
class MergeExecutionMetrics:
    """Detailed mutation metrics produced by a table Delta MERGE operation."""

    source_table: str
    total_batch_events: int
    superseded_events: int
    candidate_winners: int
    applied_inserts: int
    applied_updates: int
    applied_deletes: int
    stale_noops: int
    already_applied: int
    recovered_events: int


def collapse_batch_mutations(
    parsed_df: DataFrame,
    source_table: str,
) -> Tuple[DataFrame, DataFrame]:
    """Collapse multiple mutations for the same business key within the same batch.

    CRITICAL MULTI-MATCH MERGE PREVENTION:
    If a batch contains multiple updates for the same business key (e.g. SUB-100 at seq 501 and seq 502),
    passing both directly into a single Delta MERGE statement would violate Spark's merge semantics
    by producing an ambiguous multi-match.

    We resolve this by partitioning by business_key and ordering by source_sequence DESC, event_id DESC.
    - Highest sequence row becomes the candidate winner.
    - Subordinate rows are marked as SUPERSEDED_WITHIN_BATCH and preserved in the audit ledger.

    Returns:
        (candidate_winners_df, superseded_events_df)
    """
    pk_col = TARGET_PRIMARY_KEYS[source_table]

    window_spec = Window.partitionBy("source_table", pk_col).orderBy(
        F.col("source_sequence").desc(),
        F.col("event_id").desc(),
    )

    ranked_df = parsed_df.withColumn("_batch_rank", F.row_number().over(window_spec))

    candidate_winners_df = ranked_df.filter(F.col("_batch_rank") == 1).drop("_batch_rank")
    superseded_events_df = ranked_df.filter(F.col("_batch_rank") > 1).drop("_batch_rank")

    return candidate_winners_df, superseded_events_df


def classify_events_and_apply_merge(
    spark: SparkSession,
    source_table: str,
    parsed_batch_df: DataFrame,
    batch_id: int,
    delta_dir: Optional[Path] = None,
    audit_dir: Optional[Path] = None,
    existing_ledger_event_ids: Optional[set] = None,
) -> Tuple[DataFrame, MergeExecutionMetrics]:
    """Execute Delta MERGE for a source table with tombstone delete handling and conflict guards.

    MERGE CONTRACT:
    1. MATCHED AND source_sequence > target._last_source_sequence AND op IN ('INSERT', 'UPDATE'):
       -> Updates current row, resets _is_deleted = false, sets full after-image.
    2. MATCHED AND source_sequence > target._last_source_sequence AND op = 'DELETE':
       -> Updates row setting _is_deleted = true, preserves previous business attributes for traceability.
    3. NOT MATCHED AND op IN ('INSERT', 'UPDATE'):
       -> Inserts new row with _is_deleted = false.
    4. NOT MATCHED AND op = 'DELETE':
       -> Inserts protective minimal tombstone with _is_deleted = true and NULL business attributes.
    5. MATCHED AND source_sequence <= target._last_source_sequence:
       -> No mutation applied (stale/duplicate no-op).

    Returns:
        (audit_records_df, merge_metrics)
    """
    table_path = get_current_table_path(source_table, delta_dir)
    pk_col = TARGET_PRIMARY_KEYS[source_table]
    proc_time = datetime.now(timezone.utc).isoformat()

    # Step 1: Separate winning candidate mutations from superseded in-batch changes
    candidate_winners_df, superseded_df = collapse_batch_mutations(parsed_batch_df, source_table)

    total_batch_events = parsed_batch_df.count()
    superseded_count = superseded_df.count()
    candidate_count = candidate_winners_df.count()

    # Step 2: Distributed pre-merge target projection (NO DRIVER COLLECTION)
    target_df = spark.read.format("delta").load(str(table_path))
    target_cols = set(target_df.columns)

    target_proj_fields = [
        F.col(pk_col).alias("_tgt_pk"),
        F.col("_last_source_sequence").alias("_tgt_seq"),
        F.col("_last_event_id").alias("_tgt_event_id"),
        F.col("_is_deleted").alias("_tgt_is_deleted"),
    ]
    if "_last_schema_version" in target_cols:
        target_proj_fields.append(F.col("_last_schema_version").alias("_tgt_schema_version"))

    target_projection = target_df.select(*target_proj_fields)

    # Distributed applied events ledger projection (NO DRIVER COLLECTION)
    ledger_path = get_applied_events_path(audit_dir)
    if ledger_path.exists() and DeltaTable.isDeltaTable(spark, str(ledger_path)):
        ledger_projection = (
            spark.read.format("delta")
            .load(str(ledger_path))
            .select(F.col("event_id").alias("_ledger_event_id"))
        )
    else:
        from pyspark.sql.types import StringType, StructField, StructType

        ledger_projection = spark.createDataFrame(
            [], StructType([StructField("_ledger_event_id", StringType(), True)])
        )

    # Perform distributed joins
    joined_candidates = candidate_winners_df.join(
        target_projection, candidate_winners_df[pk_col] == target_projection["_tgt_pk"], "left"
    ).join(
        ledger_projection,
        candidate_winners_df["event_id"] == ledger_projection["_ledger_event_id"],
        "left",
    )

    # Define outcome classification expression
    is_partially_committed_expr = (
        (F.col("_tgt_pk").isNotNull())
        & (F.col("event_id") == F.col("_tgt_event_id"))
        & (F.col("source_sequence") == F.col("_tgt_seq"))
    )

    if existing_ledger_event_ids is not None:
        is_in_ledger = (
            F.col("event_id").isin(list(existing_ledger_event_ids))
            if existing_ledger_event_ids
            else F.lit(False)
        )
    else:
        is_in_ledger = F.col("_ledger_event_id").isNotNull()

    is_already_applied_expr = is_partially_committed_expr & is_in_ledger
    is_recovered_expr = is_partially_committed_expr & (~is_in_ledger)

    outcome_expr = (
        F.when(is_already_applied_expr, F.lit(OUTCOME_ALREADY_APPLIED))
        .when(is_recovered_expr, F.lit(OUTCOME_RECOVERED_AFTER_PARTIAL_COMMIT))
        .when(
            (F.col("_tgt_pk").isNotNull()) & (F.col("source_sequence") <= F.col("_tgt_seq")),
            F.lit(OUTCOME_STALE_NOOP),
        )
        .when(F.col("operation") == "INSERT", F.lit(OUTCOME_APPLIED_INSERT))
        .when(F.col("operation") == "UPDATE", F.lit(OUTCOME_APPLIED_UPDATE))
        .when(F.col("operation") == "DELETE", F.lit(OUTCOME_APPLIED_DELETE))
        .otherwise(F.lit("UNKNOWN"))
    )

    classified_candidates = (
        joined_candidates.withColumn("apply_outcome", outcome_expr)
        .withColumn("applied_at", F.lit(proc_time))
        .withColumn("target_sequence_before", F.col("_tgt_seq"))
        .withColumn(
            "target_sequence_after",
            F.when(
                F.col("apply_outcome").isin(
                    [
                        OUTCOME_APPLIED_INSERT,
                        OUTCOME_APPLIED_UPDATE,
                        OUTCOME_APPLIED_DELETE,
                    ]
                ),
                F.col("source_sequence"),
            ).otherwise(F.col("_tgt_seq")),
        )
        .withColumn(
            "reason",
            F.when(
                F.col("apply_outcome") == OUTCOME_STALE_NOOP, F.lit("SOURCE_SEQUENCE_LEQ_TARGET")
            )
            .when(
                F.col("apply_outcome") == OUTCOME_RECOVERED_AFTER_PARTIAL_COMMIT,
                F.lit("RECOVERED_AFTER_UNRECORDED_TARGET_COMMIT"),
            )
            .when(
                F.col("apply_outcome") == OUTCOME_ALREADY_APPLIED,
                F.lit("ALREADY_APPLIED_IN_TARGET_AND_LEDGER"),
            )
            .otherwise(F.lit(None)),
        )
    ).localCheckpoint(eager=True)

    # Step 3: Build audit records for superseded events
    classified_superseded = (
        superseded_df.withColumn("apply_outcome", F.lit(OUTCOME_SUPERSEDED_WITHIN_BATCH))
        .withColumn("applied_at", F.lit(proc_time))
        .withColumn("target_sequence_before", F.lit(None).cast("long"))
        .withColumn("target_sequence_after", F.lit(None).cast("long"))
        .withColumn(
            "reason",
            F.lit("SUPERSEDED_BY_HIGHER_SEQUENCE_EVENT_IN_SAME_BATCH"),
        )
    )

    # Standardize audit record columns
    audit_cols = [
        "event_id",
        "source_table",
        "business_key",
        "source_sequence",
        "operation",
        "batch_id",
        "event_timestamp",
        "apply_outcome",
        "applied_at",
        "target_sequence_before",
        "target_sequence_after",
        "reason",
    ]

    all_audit_df = (
        classified_candidates.select(*audit_cols)
        .unionByName(classified_superseded.select(*audit_cols))
        .cache()
    )

    # Calculate metrics
    outcome_counts = all_audit_df.groupBy("apply_outcome").count().collect()
    counts_map = {row["apply_outcome"]: row["count"] for row in outcome_counts}

    applied_inserts = counts_map.get(OUTCOME_APPLIED_INSERT, 0)
    applied_updates = counts_map.get(OUTCOME_APPLIED_UPDATE, 0)
    applied_deletes = counts_map.get(OUTCOME_APPLIED_DELETE, 0)
    stale_noops = counts_map.get(OUTCOME_STALE_NOOP, 0)
    already_applied = counts_map.get(OUTCOME_ALREADY_APPLIED, 0)
    recovered_events = counts_map.get(OUTCOME_RECOVERED_AFTER_PARTIAL_COMMIT, 0)

    metrics = MergeExecutionMetrics(
        source_table=source_table,
        total_batch_events=total_batch_events,
        superseded_events=superseded_count,
        candidate_winners=candidate_count,
        applied_inserts=applied_inserts,
        applied_updates=applied_updates,
        applied_deletes=applied_deletes,
        stale_noops=stale_noops,
        already_applied=already_applied,
        recovered_events=recovered_events,
    )

    # Step 4: Execute Delta MERGE on Current-State Table
    mutating_candidates_df = classified_candidates.filter(
        F.col("apply_outcome").isin(
            [
                OUTCOME_APPLIED_INSERT,
                OUTCOME_APPLIED_UPDATE,
                OUTCOME_APPLIED_DELETE,
            ]
        )
    )

    if mutating_candidates_df.count() > 0:
        delta_table = DeltaTable.forPath(spark, str(table_path))
        source_cols = set(mutating_candidates_df.columns)

        # Available business columns in source that target also contains
        available_business_cols = [
            c
            for c in target_cols
            if c in source_cols and c not in LINEAGE_FIELD_NAMES and c != pk_col
        ]

        # Setup Update dictionary for INSERT / UPDATE
        update_set: Dict[str, str] = {c: f"source.{c}" for c in available_business_cols}
        update_set.update(
            {
                "_last_source_sequence": "source.source_sequence",
                "_last_event_id": "source.event_id",
                "_last_event_timestamp": "source.event_timestamp",
                "_last_batch_id": f"cast({batch_id} as long)",
                "_updated_at": f"'{proc_time}'",
                "_is_deleted": "false",
                "_last_schema_version": "cast(source.schema_version as int)",
            }
        )

        # Setup Delete tombstone dictionary (preserves existing target business attributes)
        delete_tombstone_set: Dict[str, str] = {
            "_last_source_sequence": "source.source_sequence",
            "_last_event_id": "source.event_id",
            "_last_event_timestamp": "source.event_timestamp",
            "_last_batch_id": f"cast({batch_id} as long)",
            "_updated_at": f"'{proc_time}'",
            "_is_deleted": "true",
            "_last_schema_version": "cast(source.schema_version as int)",
        }

        # Setup Insert dictionary for new active records
        insert_set: Dict[str, str] = {pk_col: f"source.{pk_col}"}
        for c in available_business_cols:
            insert_set[c] = f"source.{c}"
        insert_set.update(
            {
                "_last_source_sequence": "source.source_sequence",
                "_last_event_id": "source.event_id",
                "_last_event_timestamp": "source.event_timestamp",
                "_last_batch_id": f"cast({batch_id} as long)",
                "_updated_at": f"'{proc_time}'",
                "_is_deleted": "false",
                "_last_schema_version": "cast(source.schema_version as int)",
            }
        )

        # Setup Protective Tombstone dictionary for unknown keys deleted before creation
        protective_tombstone_set: Dict[str, str] = {pk_col: f"source.{pk_col}"}
        for col in target_cols:
            if col != pk_col and col not in LINEAGE_FIELD_NAMES:
                protective_tombstone_set[col] = "null"
        protective_tombstone_set.update(
            {
                "_last_source_sequence": "source.source_sequence",
                "_last_event_id": "source.event_id",
                "_last_event_timestamp": "source.event_timestamp",
                "_last_batch_id": f"cast({batch_id} as long)",
                "_updated_at": f"'{proc_time}'",
                "_is_deleted": "true",
                "_last_schema_version": "cast(source.schema_version as int)",
            }
        )

        merge_condition = f"target.{pk_col} = source.{pk_col}"

        (
            delta_table.alias("target")
            .merge(
                source=mutating_candidates_df.alias("source"),
                condition=merge_condition,
            )
            .whenMatchedUpdate(
                condition=(
                    "source.source_sequence > target._last_source_sequence "
                    "AND source.operation IN ('INSERT', 'UPDATE')"
                ),
                set=update_set,
            )
            .whenMatchedUpdate(
                condition=(
                    "source.source_sequence > target._last_source_sequence "
                    "AND source.operation = 'DELETE'"
                ),
                set=delete_tombstone_set,
            )
            .whenNotMatchedInsert(
                condition="source.operation IN ('INSERT', 'UPDATE')",
                values=insert_set,
            )
            .whenNotMatchedInsert(
                condition="source.operation = 'DELETE'",
                values=protective_tombstone_set,
            )
            .execute()
        )

    return all_audit_df, metrics
