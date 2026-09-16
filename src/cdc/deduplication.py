"""Deterministic event deduplication engine."""

from typing import Tuple

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


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

    Returns:
        (deduped_df, duplicates_df)
    """
    # Deterministic tie-breaking order: earliest ingestion timestamp, then lowest sequence
    window_spec = Window.partitionBy("event_id").orderBy(
        F.col("ingested_timestamp").asc_nulls_last(),
        F.col("source_sequence").asc(),
    )

    ranked_df = df.withColumn("_row_num", F.row_number().over(window_spec))

    deduped_df = ranked_df.filter(F.col("_row_num") == 1).drop("_row_num")
    duplicates_df = ranked_df.filter(F.col("_row_num") > 1).drop("_row_num")

    return deduped_df, duplicates_df
