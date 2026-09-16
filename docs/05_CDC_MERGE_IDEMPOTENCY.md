# CDC MERGE Idempotency & Failure Recovery Architecture

This document explains how the Incremental CDC pipeline achieves **idempotent, effectively-once processing** across multi-table mutations, crash gaps, and batch re-executions.

---

## 1. The Core Challenge: Lack of Cross-Table ACID

In Lakehouse systems like Delta Lake, ACID transactions are scoped to a **single table**. 

Updating a Current-State table (`delta/current/subscriptions`), recording an event in an Applied Event Ledger (`delta/audit/applied_events`), and persisting a Global Checkpoint (`state/checkpoints/checkpoint_state.json`) represent operations across **multiple distinct storage targets**.

```
Step 1: Delta MERGE to Current-State Table   ──► SUCCESS
                         │
        [ CRASH / FAILURE WINDOW HERE ]
                         │
Step 2: Delta MERGE to Applied Event Ledger  ──► NEVER EXECUTED
Step 3: Commit Global Batch Checkpoint       ──► NEVER EXECUTED
```

If the pipeline process terminates between Step 1 and Step 3:
- The target table contains updated state (`_last_event_id`, `_last_source_sequence`).
- The applied event ledger has missing audit rows.
- The checkpoint remains at the previous batch boundary.

When the batch is re-executed, a naive pipeline would attempt to re-apply the changes, potentially corrupting sequence tracking or throwing concurrency errors. Our architecture is explicitly designed to handle this failure mode and recover deterministically.

---

## 2. Terminology & Semantics: Exactly-Once vs Effectively-Once

> [!IMPORTANT]
> In distributed architectures spanning independent storage systems, true end-to-end "exactly-once" delivery is physically impossible without coordinated two-phase commit (2PC) protocols across all systems.
> 
> Therefore, this pipeline provides **idempotent / effectively-once behavior for unchanged inputs**.
> 
> When the same batch is re-run (or retried after a partial failure), the resulting system state—current-state tables, event audit records, and checkpoints—is identical to a single successful execution.

---

## 3. The Mutation Lifecycle & Classification

Every incoming valid CDC event passes through a deterministic classification pipeline before mutating state:

```
                          Incoming Valid CDC Event
                                     │
                      [ Group by Table + Business Key ]
                                     │
             ┌───────────────────────┴───────────────────────┐
             │                                               │
Earlier Sequence in Batch                       Highest Sequence in Batch
             │                                               │
             ▼                                               ▼
  SUPERSEDED_WITHIN_BATCH                             Candidate Winner
(Audited, no target mutation)                                │
                                             [ Compare with Target State ]
                                                             │
                  ┌──────────────────────┬───────────────────┴───────────────────┐
                  │                      │                                       │
     source_seq < target_seq     source_seq == target_seq               source_seq > target_seq
                  │                      │                                       │
                  ▼                      ▼                                       ▼
             STALE_NOOP           ALREADY_APPLIED /                      Delta MERGE Mutation
        (Audited, no mutation)   RECOVERED_AFTER_PARTIAL_COMMIT          - APPLIED_INSERT
                                 (Ledger repaired, no mutation)          - APPLIED_UPDATE
                                                                         - APPLIED_DELETE
```

### 3.1. Within-Batch Event Collapsing (`SUPERSEDED_WITHIN_BATCH`)
When multiple changes for the same business key appear in the same batch (e.g., `UPDATE plan=BASIC` at seq 501, followed by `UPDATE plan=PRO` at seq 502):
1. The batch cannot pass both records into a single Delta MERGE statement, as Delta throws a runtime exception when multiple source rows match the same target row.
2. The pipeline partitions by `(source_table, business_key)` and selects the winning event using `ROW_NUMBER() OVER (ORDER BY source_sequence DESC, event_id DESC)`.
3. The winner (seq 502) is submitted to the MERGE.
4. The earlier event (seq 501) is audited with outcome `SUPERSEDED_WITHIN_BATCH`. It is preserved in the audit ledger for historical traceability, but never touches the target table.

### 3.2. Source-Sequence Conflict Resolution (`STALE_NOOP`)
If an event arrives with a `source_sequence` lower than `target._last_source_sequence`:
1. The target already contains newer information.
2. Overwriting newer data with an older change would violate serializability.
3. The mutation is blocked by the MERGE condition (`source.source_sequence > target._last_source_sequence`).
4. The event is audited with outcome `STALE_NOOP` (or flagged for Module 3 late-event replay).

### 3.3. Target/Ledger Crash Recovery (`RECOVERED_AFTER_PARTIAL_COMMIT`)
If a previous execution succeeded in updating the target table but crashed before writing the event ledger or advancing the checkpoint:
1. On retry, the pipeline checks the target row before executing the MERGE.
2. It detects that `target._last_event_id == candidate.event_id` and `target._last_source_sequence == candidate.source_sequence`.
3. The pipeline recognizes that the business mutation was already applied.
4. It bypasses target mutation and upserts the missing audit record into `applied_events` with outcome `RECOVERED_AFTER_PARTIAL_COMMIT`.
5. Once all tables and audit ledgers succeed, the global checkpoint cleanly advances.

---

## 4. Delete Semantics & Ordering Edge Cases

### 4.1. Tombstone Retention
When a record is deleted:
- It is **not** physically removed via `DELETE FROM`.
- Instead, the row is updated with `_is_deleted = true`, advancing `_last_source_sequence` to the delete event's sequence.
- All existing business attributes are preserved.
- **Why**: Retaining the tombstone preserves sequence memory. If an older `UPDATE` or `INSERT` arrives later due to network latency, the sequence guard compares `incoming.source_sequence` against `tombstone._last_source_sequence` and safely rejects the stale mutation.

### 4.2. Delete for Unknown Key (Protective Tombstone)
If a `DELETE` event arrives for a business key that does not exist in the target table:
- A new protective tombstone row is inserted with `_is_deleted = true`, setting `_last_source_sequence = delete.source_sequence` and all business attributes to `NULL`.
- **Scenario**:
  1. Sequence 100: `DELETE SUB-999` arrives first (due to out-of-order delivery). Target does not have `SUB-999`.
  2. Protective tombstone created: `SUB-999`, `_is_deleted = true`, `_last_source_sequence = 100`.
  3. Sequence 90: `INSERT SUB-999` arrives later.
  4. Guard evaluates: `90 > 100` is `FALSE`. The stale insert is rejected as `STALE_NOOP`.

### 4.3. Re-Insert After Delete
If an entity is deleted and subsequently recreated in the source OLTP system:
- The source generates a new `INSERT` event with a **higher** sequence number (e.g. `seq 710 > seq 700`).
- The Delta MERGE `whenMatchedUpdate` condition matches: `source.source_sequence > target._last_source_sequence AND source.operation IN ('INSERT', 'UPDATE')`.
- The existing tombstone row is updated in-place:
  - `_is_deleted` is reset to `false`.
  - Lineage fields (`_last_source_sequence`, `_last_event_id`, etc.) advance to the new insert.
  - Business attributes are overwritten with the new full after-image.

---

## 5. The Applied Event Ledger (`delta/audit/applied_events`)

The applied event ledger guarantees auditability and prevents duplicate processing.

### Schema Contract
```
root
 |-- event_id: string (nullable = false)
 |-- source_table: string (nullable = false)
 |-- business_key: string (nullable = false)
 |-- source_sequence: long (nullable = false)
 |-- operation: string (nullable = false)
 |-- batch_id: string (nullable = false)
 |-- event_timestamp: timestamp (nullable = false)
 |-- apply_outcome: string (nullable = false)
 |-- applied_at: timestamp (nullable = false)
 |-- target_sequence_before: long (nullable = true)
 |-- target_sequence_after: long (nullable = true)
 |-- reason: string (nullable = true)
```

### Upsert Idempotency
Audit events are written using Delta MERGE keyed on `event_id`:
```python
ledger_table.alias("target").merge(
    batch_audit_df.alias("source"), "target.event_id = source.event_id"
).whenNotMatchedInsertAll().execute()
```
If a batch is rerun, existing ledger entries are left untouched; any missing entries are inserted. The ledger never contains duplicate `event_id` records.

---

## 6. Batch-Atomic Global Checkpoint (`checkpoint_state.json`)

To prevent partial batch commitments across tables, the checkpoint manager utilizes a single unified checkpoint state file:

```json
{
  "version": 1,
  "tables": {
    "accounts": { "highest_source_sequence": 1500, "last_processed_batch_id": "batch_001" },
    "subscriptions": { "highest_source_sequence": 1820, "last_processed_batch_id": "batch_001" },
    "invoices": { "highest_source_sequence": 1950, "last_processed_batch_id": "batch_001" },
    "payments": { "highest_source_sequence": 2100, "last_processed_batch_id": "batch_001" }
  },
  "completed_batch_ids": ["batch_001"]
}
```

### Checkpoint Commit Rules
1. **End-of-Batch Execution**: The checkpoint is committed **only after** all four target tables have merged successfully, the applied event ledger has merged, and batch metrics have persisted.
2. **Regression Rejection**: Proposed high-water marks are validated against current state in memory. If any table's proposed sequence is lower than its recorded high-water mark, the entire batch commit is aborted before touching disk.
3. **Atomic Replacement**: The new state is written to a temporary file, flushed with `os.fsync()`, and atomically replaced via `os.replace()`.
4. **All-or-Nothing Progress**: Either all tables advance to the new batch boundary, or none advance.
