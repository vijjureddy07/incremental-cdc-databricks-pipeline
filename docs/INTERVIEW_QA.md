# Interview Questions & Answers: CDC & Incremental Systems

> **Learning Status**: NOT STUDIED / PENDING

---

### Q1: What is Change Data Capture (CDC)?
**Answer**:
Change Data Capture (CDC) is a pattern that identifies and captures row-level state changes (INSERT, UPDATE, DELETE) from an operational source database's transaction log (WAL/binlog) and streams those changes to analytical downstream systems with minimal latency and near-zero impact on OLTP performance.

---

### Q2: What is the difference between CDC and a full refresh?
**Answer**:
A full refresh scans and rewrites the entire dataset on every execution, resulting in compute and I/O costs that scale linearly ($O(N)$) with total historical data volume. CDC captures only the net changes since the last run ($O(\Delta N)$), reducing compute runtimes, cloud storage egress, and target table lock times from hours to seconds.

---

### Q3: How does CDC differ from traditional timestamp-based incremental loading?
**Answer**:
Traditional incremental loading relies on queries like `WHERE updated_at > :last_watermark`. This approach has three fatal flaws:
1. **Missed Deletes**: It cannot capture hard `DELETE` operations (since deleted rows no longer exist to query).
2. **Intermediate Overwrites**: Multiple updates between extraction runs are collapsed into a single row, losing audit history.
3. **Clock Skew / Uncommitted Transactions**: In-flight transactions committed with earlier timestamps are missed.
CDC overcomes all three by streaming directly from the immutable transaction log in commit sequence.

---

### Q4: What is an LSN?
**Answer**:
A Log Sequence Number (LSN) is an integer or byte offset indicating the exact physical position of a transaction record within a database Write-Ahead Log (WAL). LSNs are monotonically increasing and provide an immutable, linear timeline of all committed mutations.

---

### Q5: Why is physical arrival order unsafe for CDC processing?
**Answer**:
Distributed messaging queues (like Kafka with multiple partitions) and cloud object storage writes do not guarantee that records arrive in the exact order they were committed. Network retries and concurrent workers can deliver an older update after a newer update. Using arrival order causes older data to overwrite newer data.

---

### Q6: Why use a `source_sequence` field?
**Answer**:
`source_sequence` simulates the database transaction log position (LSN). By ordering events by `source_sequence` within each `(source_table, business_key)`, the consumer guarantees that state transitions are replayed in the exact logical order executed at the source.

---

### Q7: What makes an event a duplicate?
**Answer**:
An event is a duplicate when the exact same mutation event (identical `source_table`, `business_key`, `source_sequence`, and `operation`) is received more than once. This commonly occurs due to at-least-once delivery semantics or producer retries.

---

### Q8: What is the difference between a business key and an event ID?
**Answer**:
- **Business Key**: Identifies the persistent domain entity (e.g. `account_id = ACC-001`). It remains unchanged across all lifecycle states of that entity.
- **Event ID**: Identifies a single, discrete point-in-time state mutation for that entity, deterministically hashed from `SHA256(table + business_key + source_sequence + operation)`.

---

### Q9: How are DELETE operations represented in CDC?
**Answer**:
DELETE operations are emitted as "tombstone" events. The payload contains only the fields required to identify the record (the primary/business key) and deletion metadata. Downstream consumers use this tombstone to apply soft-delete flags (`is_deleted = true`) or execute Delta Lake `DELETE WHERE` / `WHEN MATCHED THEN DELETE`.

---

### Q10: What is a checkpoint?
**Answer**:
A checkpoint is a persistent metadata record tracking the high-water mark, batch IDs, and processing state committed by a pipeline. It defines the exact boundary from which the next incremental run will resume.

---

### Q11: What is a high-water mark (HWM)?
**Answer**:
A high-water mark represents the highest source sequence number or timestamp successfully ingested and committed for a given dataset. Any event with a sequence above the HWM is eligible for processing in the next incremental batch.

---

### Q12: What happens when a late-arriving change appears below the checkpoint?
**Answer**:
When an event arrives with a `source_sequence <= current_checkpoint_hwm`, it is classified as a `LATE_EVENT`. It must **never** be silently discarded because it represents valid real-world data. It is routed to an isolated storage path (`late_events/`) for reconciliation, historical replay, or SCD Type 2 backfilling (implemented in Module 3).

---

### Q13: Why must the checkpoint advance ONLY after successful processing?
**Answer**:
If the checkpoint advances before or during processing and a failure occurs (e.g., node crash, network drop, schema error), the pipeline on restart will resume from the advanced position, permanently skipping the failed records. Advancing only after data write and verification guarantees at-least-once processing without data loss.

---

### Q14: What does idempotency mean in data engineering?
**Answer**:
An operation is idempotent if executing it multiple times yields the exact same outcome as executing it once. In CDC pipelines, idempotency is achieved by tracking processed batch IDs, deduplicating via deterministic event IDs, and applying upserts (`MERGE INTO`) rather than blind appends.
