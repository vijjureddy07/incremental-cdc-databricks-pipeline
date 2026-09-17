"""Deterministic event deduplication engine."""

from typing import Tuple

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def compute_canonical_event_hash_expr():
    """Build PySpark expression for SHA-256 canonical hash of envelope fields and payload."""
    return F.sha2(
        F.concat_ws(
            "||",
            F.coalesce(F.col("event_id"), F.lit("")),
            F.coalesce(F.col("source_table"), F.lit("")),
            F.coalesce(F.col("operation"), F.lit("")),
            F.coalesce(F.col("business_key"), F.lit("")),
            F.coalesce(F.col("source_sequence").cast("string"), F.lit("")),
            F.coalesce(F.col("event_timestamp"), F.lit("")),
            F.coalesce(F.col("ingested_timestamp"), F.lit("")),
            F.coalesce(F.col("batch_id").cast("string"), F.lit("")),
            F.coalesce(F.col("schema_version").cast("string"), F.lit("")),
            F.coalesce(F.col("payload"), F.lit("")),
        ),
        256,
    )


def add_canonical_event_hash(df: DataFrame, col_name: str = "canonical_event_hash") -> DataFrame:
    """Add a deterministic SHA-256 canonical hash of all envelope fields and payload."""
    return df.withColumn(col_name, compute_canonical_event_hash_expr())


def deduplicate_cdc_events(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Deduplicate CDC events deterministically by event_id.

    CRITICAL ARCHITECTURAL DISTINCTION:
    1. DUPLICATE EVENT:
       Identical event_id appears more than once in the change feed (e.g. from network retries,
       at-least-once producer semantics, or repeated batch ingestion).
       We resolve this by deterministic ranking via ROW_NUMBER() over (PARTITION BY event_id).

    2. MULTIPLE LEGITIMATE UPDATES TO THE SAME BUSINESS KEY:
       A business entity (e.g. subscription S-100) receiving sequence 15 (BASIC -> PRO)
       followed by sequence 16 (PRO -> ENTERPRISE). Because event_id incorporates source_sequence,
       these two changes yield completely distinct event_ids and are BOTH safely preserved!

    WHY NOT `dropDuplicates(['event_id'])`?
    In Apache Spark, `dropDuplicates` across distributed partitions does not guarantee WHICH
    physical record is retained when non-keyed columns differ. Using `ROW_NUMBER()` with an explicit
    `ORDER BY` clause provides strict, reproducible determinism.

    STABLE TIE-BREAKER:
    To guarantee 100% determinism even when duplicate event IDs share identical
    `ingested_timestamp` and `source_sequence` but differ in metadata or payload, we compute
    `canonical_event_hash` (SHA-256 over all envelope fields and payload) as the final tie-breaker.

    Returns:
        (deduped_df, duplicates_df)
    """
    df_with_hash = add_canonical_event_hash(df, "_canonical_event_hash")

    # Deterministic tie-breaking order:
    # 1. Earliest ingestion timestamp
    # 2. Lowest source sequence
    # 3. Stable canonical hash of complete envelope & payload
    window_spec = Window.partitionBy("event_id").orderBy(
        F.col("ingested_timestamp").asc_nulls_last(),
        F.col("source_sequence").asc(),
        F.col("_canonical_event_hash").asc(),
    )

    ranked_df = df_with_hash.withColumn("_row_num", F.row_number().over(window_spec))

    deduped_df = (
        ranked_df.filter(F.col("_row_num") == 1).drop("_row_num").drop("_canonical_event_hash")
    )
    duplicates_df = (
        ranked_df.filter(F.col("_row_num") > 1).drop("_row_num").drop("_canonical_event_hash")
    )

    return deduped_df, duplicates_df
