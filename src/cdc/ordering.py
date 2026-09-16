"""CDC event ordering engine."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def order_cdc_events(df: DataFrame) -> DataFrame:
    """Order CDC events deterministically by source_sequence.

    CORE PRINCIPLE:
    In distributed systems, file arrival order, network packets, or partition scanning order
    can arrive out-of-order physically. CDC logic MUST NEVER rely on arrival order.
    Events must be strictly ordered by `source_sequence` within each (source_table, business_key).
    """
    return df.orderBy(
        F.col("source_table").asc(),
        F.col("business_key").asc(),
        F.col("source_sequence").asc(),
    )
