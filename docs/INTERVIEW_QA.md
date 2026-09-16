# Interview Questions & Answers: CDC, Delta MERGE & Lakehouse Architecture

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
DELETE operations are emitted as "tombstone" events. The payload contains only the fields required to identify the record (the primary/business key) and deletion metadata. Downstream consumers use this tombstone to apply soft-delete flags (`_is_deleted = true`) or execute Delta Lake `DELETE WHERE` / `WHEN MATCHED THEN DELETE`.

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

---

### Q15: Why can't multiple source rows matching one target be blindly passed to MERGE?
**Answer**:
Delta Lake's MERGE specification requires deterministic 1-to-1 matching between incoming source rows and existing target rows. If an incoming batch contains two distinct events for the same business key (e.g. `UPDATE plan=BASIC` at seq 501 and `UPDATE plan=PRO` at seq 502), the MERGE engine cannot decide which row's values should update the target, throwing `DeltaMergeException: Cannot perform MERGE as multiple source rows matched the same target row`.

---

### Q16: Why collapse CDC to final mutation for current-state processing?
**Answer**:
A current-state table represents the single latest known state for each business key. When a batch contains multiple mutations for the same key, only the event with the highest valid `source_sequence` determines the latest after-image. Collapsing the mutations to the final winner guarantees that exactly one row per business key enters the Delta MERGE, satisfying Delta's 1-to-1 match constraint while correctly advancing target state.

---

### Q17: What happens to intermediate changes when collapsing in-batch mutations?
**Answer**:
Intermediate changes are **not** discarded. While only the winning after-image mutates the current-state table, every valid incoming event is audited and persisted to the Applied Event Ledger (`delta/audit/applied_events`). Intermediate events are classified with the outcome `SUPERSEDED_WITHIN_BATCH`. This preserves complete auditability and lineage for regulatory compliance and downstream SCD2 history generation.

---

### Q18: Why retain delete tombstones in the current-state Delta table?
**Answer**:
Retaining delete tombstones (`_is_deleted = true`) keeps the sequence history of the deletion directly on the target table. If an out-of-order or stale `UPDATE` arrives later (with `seq < delete_seq`), the sequence guard can compare `incoming.source_sequence > target._last_source_sequence` and reject the stale update. If the row were physically deleted, sequence memory would be lost and the stale update would erroneously recreate the record.

---

### Q19: Physical delete vs soft delete in Lakehouse architectures?
**Answer**:
- **Physical Delete (`DELETE FROM` / `whenMatchedDelete()`)**: Permanently removes rows from latest table version. Pros: removes clutter, complies with GDPR right-to-be-forgotten. Cons: erases target sequence memory, makes pipeline crashes during audit/checkpoint updates non-recoverable, and prevents downstream consumers from detecting what was deleted without scanning transaction log diffs.
- **Soft Delete (Tombstone with `_is_deleted = true`)**: Retains the row shell, lineage fields (`_last_source_sequence`, `_last_event_id`), and historical attributes while flagging `_is_deleted = true`. Active records are retrieved via views (`WHERE _is_deleted = false`). Pros: preserves sequence memory for crash recovery, protects against stale replays, and simplifies downstream incremental consumption.

---

### Q20: Why compare `source_sequence` to target last sequence in MERGE conditions?
**Answer**:
Distributed networks, concurrent batch queues, and consumer retries frequently deliver change events out of commit order. In Delta MERGE, specifying `WHEN MATCHED AND source.source_sequence > target._last_source_sequence` ensures that an incoming event can only mutate the target if it represents a strictly newer state transition. Stale or older events evaluate to false and cause zero target mutation.

---

### Q21: What happens if the target commits but the audit ledger fails?
**Answer**:
This is the classic multi-table crash gap. The current-state table has already advanced to the new sequence and event ID, but the applied event ledger lacks the audit record, and the batch checkpoint has not advanced. On retry, the pipeline inspects the target row before executing the MERGE, recognizes that `target._last_event_id == candidate.event_id`, bypasses target mutation, and repairs the missing audit entry in `applied_events` with outcome `RECOVERED_AFTER_PARTIAL_COMMIT`. Once all tables and ledgers succeed, the batch checkpoint commits.

---

### Q22: Why is Delta ACID not cross-table ACID?
**Answer**:
Delta Lake ACID guarantees are strictly scoped to a single table's transaction log (`<table_dir>/_delta_log/`). Delta does not provide a distributed two-phase commit (2PC) or multi-table transaction coordinator across independent table paths. Updating `accounts`, `subscriptions`, and `applied_events` involves three separate Delta commits. Pipelines must be architected with target-state inspection, idempotent MERGE logic, and atomic checkpointing to achieve crash consistency across tables.

---

### Q23: What does idempotent CDC application mean?
**Answer**:
Idempotent CDC application means that processing a batch of CDC events multiple times produces the exact same final current-state tables, the exact same set of unique audit ledger rows, and the exact same checkpoint state as processing it once. Any already-applied mutations evaluate to no-ops or recovered audits without altering row counts or mutating values.

---

### Q24: Exactly-once vs effectively-once in Lakehouse pipelines?
**Answer**:
"Exactly-once delivery" is an impossible theoretical guarantee across heterogeneous distributed systems subject to network partitions (FLM/CAP theorem). What data engineering systems achieve is **effectively-once processing**: underlying transport delivers events with at-least-once semantics, but deterministic deduplication, idempotent upserts (`MERGE`), and high-water mark sequence guards ensure that duplicate deliveries produce zero side effects on target state.

---

### Q25: How does a reinsert after delete work?
**Answer**:
When an entity is deleted and later re-created in the OLTP database, a new `INSERT` event is generated with a higher `source_sequence` than the preceding `DELETE`. In the current-state Delta table:
1. The incoming `INSERT` matches the existing tombstone row on the business key.
2. The sequence guard condition passes (`insert.source_sequence > tombstone._last_source_sequence`).
3. The row is updated in-place: `_is_deleted` is flipped back to `false`, lineage fields advance to the new event, and business fields are populated with the new after-image.
