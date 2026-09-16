# 03: Event Ordering, Deduplication & Late-Event Handling

> **Learning Status**: NOT STUDIED / PENDING

---

## 1. Duplicate Event vs Legitimate Multiple Updates

One of the most dangerous design bugs in data pipelines is conflating a duplicate event with multiple updates to the same entity.

### Scenario Comparison

```
Entity: Subscription SUB-100

Scenario A: Duplicate Event (Bug/Retry)
Batch 01: [event_id: e1, seq: 15, plan: 'PRO',  ingested: 10:00]
Batch 01: [event_id: e1, seq: 15, plan: 'PRO',  ingested: 10:01]
-> Same sequence, same operation, same event_id.
-> ACTION: Deduplicate! Retain exactly 1 row.

Scenario B: Legitimate Multiple Updates (Rapid Business Change)
Batch 01: [event_id: e1, seq: 15, plan: 'PRO',        ingested: 10:00]
Batch 01: [event_id: e2, seq: 16, plan: 'ENTERPRISE', ingested: 10:05]
-> Different sequences, different timestamps, DIFFERENT event_ids!
-> ACTION: PRESERVE BOTH! They represent the evolutionary history of SUB-100.
```

If deduplication is incorrectly applied on `business_key` alone (`dropDuplicates(["business_key"])`), the pipeline will delete legitimate intermediate states, corrupting historical audits and revenue calculations!

---

## 2. Arrival Order vs Logical Source Order

In distributed environments, file storage, and message queues:
$$\text{Physical Arrival Order} \neq \text{Logical Mutation Order}$$

### Concrete Example:
1. At $T_0$, an account is created: `ACC-001` (Sequence 100).
2. At $T_1$, the account updates its tier to `GROWTH` (Sequence 101).
3. At $T_2$, the account updates its tier to `ENTERPRISE` (Sequence 102).

Due to partition shuffling, network retries, or multi-threaded ingestion, the records physically arrive as:
$$\text{Arrival Order: } [\text{Seq 102}, \text{Seq 100}, \text{Seq 101}]$$

If a pipeline processes records in arrival order, the final saved state would be `Seq 101` (`GROWTH`), completely overwriting the true latest state `Seq 102` (`ENTERPRISE`)!

### The Solution:
The pipeline orders all valid CDC records by `source_sequence ASC` partitioned by `(source_table, business_key)` before applying any downstream state mutations:
```python
ordered_df = df.orderBy(
    F.col("source_table").asc(),
    F.col("business_key").asc(),
    F.col("source_sequence").asc(),
)
```

---

## 3. Deterministic Deduplication with `ROW_NUMBER()`

### Why Spark's `dropDuplicates()` is Insufficient
Spark's `DataFrame.dropDuplicates(["event_id"])` drops duplicate rows across partitions. However, if non-keyed fields vary (e.g. slight variance in `ingested_timestamp` or worker node metadata), Spark makes an **arbitrary, nondeterministic choice** depending on partition layout and hash distribution.

### The Deterministic Implementation:
```python
window_spec = Window.partitionBy("event_id").orderBy(
    F.col("ingested_timestamp").asc_nulls_last(),
    F.col("source_sequence").asc(),
)

ranked_df = df.withColumn("_row_num", F.row_number().over(window_spec))
deduped_df = ranked_df.filter(F.col("_row_num") == 1).drop("_row_num")
duplicates_df = ranked_df.filter(F.col("_row_num") > 1).drop("_row_num")
```
This guarantees 100% reproducible tie-breaking: the earliest arriving ingestion record is consistently selected across repeated runs.

---

## 4. Late-Arriving Events

### What is a Late Event?
An event whose `source_sequence` is lower than or equal to the high-water mark already committed for that table in a previous batch:
$$\text{source\_sequence} \le \text{Current High-Water Mark}$$

### Example:
- Checkpoint high-water mark for `subscriptions`: `500`.
- Batch 003 contains an event for `SUB-005` with `source_sequence = 490`.

### Crucial Principle: Late Does NOT Mean Invalid!
A late event represents a legitimate real-world state change that was delayed due to an upstream network partition, mobile sync delay, or queue lag.

1. **Do NOT Discard**: The event passed schema and business validation. Dropping it silently causes state divergence.
2. **Isolation**: The pipeline isolates late events into `output/late_events/` for downstream reconciliation.
3. **Recovery Strategy**: Module 3 will implement historical replay and Delta Lake time-travel reconciliation for late-arriving events.
