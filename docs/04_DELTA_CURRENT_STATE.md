# Delta Current-State Tables & Mutation Architecture

This document details the design, storage layout, schema contracts, and mutation semantics of the **Current-State Delta Layer** in the Incremental Change Data Capture (CDC) pipeline.

---

## 1. Architectural Role: Current State vs Event History

In modern Lakehouse data architectures, there is a fundamental distinction between the **Event Log** (immutable stream of changes) and the **Current-State Table** (mutable snapshot of latest entity states).

```
Simulated OLTP Binlog / WAL
            │
            ▼
   CDC Event Change Feed (JSONL)
   - Every INSERT, UPDATE, DELETE
   - Immutable audit trail
   - Preserves intermediate states
            │
            ▼
   Delta MERGE Mutation Engine
   - Resolves within-batch collisions
   - Enforces sequence guards
   - Applies soft-delete tombstones
            │
            ▼
┌────────────────────────────────────────────────────────┐
│ Current-State Delta Tables (delta/current/*)           │
│ - One physical row per business key                    │
│ - Reflects latest valid after-image                    │
│ - Tombstoned records retained with _is_deleted = true  │
└────────────────────────────────────────────────────────┘
```

### Key Differences

| Dimension | Event History (`delta/audit/applied_events`) | Current-State Table (`delta/current/<entity>`) |
| :--- | :--- | :--- |
| **Grain** | One record per logical change event | One record per business key |
| **Mutability** | Append-only / idempotent upsert ledger | In-place upsert/tombstone mutations via `MERGE` |
| **Intermediate Changes** | Retained (e.g. 10 plan updates = 10 rows) | Collapsed (e.g. 10 plan updates = latest plan) |
| **Purpose** | Replayability, auditing, compliance | Analytical queries, operational dashboards, downstream serving |
| **Deletes** | Recorded as `APPLIED_DELETE` audit rows | Preserved as soft-deleted tombstones (`_is_deleted = true`) |

---

## 2. Core Concepts Reference Guide

### 2.1. Delta Lake
- **WHAT IT IS**: An open-source storage layer that brings ACID transactions, scalable metadata handling, and unified streaming/batch processing to Apache Spark and object stores.
- **WHY IT MATTERS**: Plain Parquet on object storage cannot guarantee atomicity during concurrent writes, cannot execute row-level `MERGE` operations without rewriting entire partitions, and suffers from read inconsistency during in-flight writes. Delta Lake solves this with its transactional log (`_delta_log`).
- **WHERE USED**: All target tables under `delta/current/` (`accounts`, `subscriptions`, `invoices`, `payments`) and audit tables (`delta/audit/applied_events`, `delta/audit/batch_apply_metrics`).
- **FILE/FUNCTION**: [src/utils/spark.py](file:///Users/vijjureddy/Library/CloudStorage/GoogleDrive-mvsreddymorthala@gmail.com/My%20Drive/Job%20Switch%20Projects/Incremental%20CDC%20Databricks%20Pipeline/src/utils/spark.py#L33-L75), [src/delta/current_state.py](file:///Users/vijjureddy/Library/CloudStorage/GoogleDrive-mvsreddymorthala@gmail.com/My%20Drive/Job%20Switch%20Projects/Incremental%20CDC%20Databricks%20Pipeline/src/delta/current_state.py#L22-L75).
- **EXAMPLE**: Initializing Delta tables via `df.write.format("delta").mode("overwrite").save(path)`.
- **INTERVIEW QUESTION**: What is Delta Lake, and what core capabilities does it add over Apache Parquet on cloud object storage?
- **EXPECTED ANSWER**: Delta Lake adds an ACID transaction log (`_delta_log`) on top of Parquet data files. This enables serializable ACID guarantees, row-level updates/deletes/merges without rewriting whole tables, time-travel queries via versioned commits, schema enforcement and schema evolution, and unified batch/streaming ingestion.

---

### 2.2. Parquet vs Delta
- **WHAT IT IS**: Apache Parquet is a columnar binary file format with dictionary encoding and run-length compression. Delta Lake is a storage format combining Parquet data files with an ordered, ACID transaction log (`_delta_log/*.json`).
- **WHY IT MATTERS**: Overwriting Parquet folders is non-atomic and susceptible to orphaned files on write failures. Delta guarantees that a commit is only visible when its commit JSON is atomically written to `_delta_log/`.
- **WHERE USED**: Core data storage across all Delta targets.
- **FILE/FUNCTION**: [src/delta/current_state.py](file:///Users/vijjureddy/Library/CloudStorage/GoogleDrive-mvsreddymorthala@gmail.com/My%20Drive/Job%20Switch%20Projects/Incremental%20CDC%20Databricks%20Pipeline/src/delta/current_state.py#L40-L65).
- **EXAMPLE**: `spark.read.format("delta").load(path)` reads the snapshot constructed from `_delta_log`, ignoring uncommitted Parquet files.
- **INTERVIEW QUESTION**: Can you perform an incremental upsert or merge directly on plain Parquet files in object storage?
- **EXPECTED ANSWER**: Plain Parquet does not support row-level upsert or merge. To modify data in plain Parquet, the query engine must perform a full partition or full table rewrite (`COPY ON WRITE`). Delta Lake provides row-level `MERGE INTO` by reading existing files, creating new versioned Parquet files containing the mutated rows, and committing the changes atomically in the transaction log.

---

### 2.3. Delta Transaction Log (`_delta_log`)
- **WHAT IT IS**: An ordered sequence of JSON commits (`000000.json`, `000001.json`, etc.) recording metadata actions (`add`, `remove`, `commitInfo`, `protocol`) for every table transaction.
- **WHY IT MATTERS**: Readers query the transaction log to determine the exact set of valid Parquet data files for a specific table version, eliminating directory listing bottlenecks and providing snapshot isolation.
- **WHERE USED**: Stored automatically inside each Delta table directory.
- **FILE/FUNCTION**: Spark Delta engine integration configured in [src/utils/spark.py](file:///Users/vijjureddy/Library/CloudStorage/GoogleDrive-mvsreddymorthala@gmail.com/My%20Drive/Job%20Switch%20Projects/Incremental%20CDC%20Databricks%20Pipeline/src/utils/spark.py#L48-L60).
- **EXAMPLE**: Each batch apply generates a new JSON commit file in `delta/current/<table_name>/_delta_log/`.
- **INTERVIEW QUESTION**: How does Delta Lake guarantee ACID properties during concurrent reads and writes?
- **EXPECTED ANSWER**: Delta Lake uses optimistic concurrency control (OCC). Writers attempt to write their changes and atomically create a new monotonic log file (`00000N.json`). If another writer committed first with conflicting changes, Delta throws a concurrency exception and retries. Readers read a consistent snapshot defined by the latest log commit at the time query execution begins.

---

### 2.4. Current-State Table
- **WHAT IT IS**: A target table that materializes the single latest known state for each business entity.
- **WHY IT MATTERS**: Downstream reporting and operational queries need the current status of an account or subscription without executing expensive grouping or window aggregations over billions of raw CDC events.
- **WHERE USED**: `accounts_current`, `subscriptions_current`, `invoices_current`, `payments_current`.
- **FILE/FUNCTION**: [src/delta/current_state.py](file:///Users/vijjureddy/Library/CloudStorage/GoogleDrive-mvsreddymorthala@gmail.com/My%20Drive/Job%20Switch%20Projects/Incremental%20CDC%20Databricks%20Pipeline/src/delta/current_state.py#L22-L75).
- **EXAMPLE**: One row with `subscription_id = SUB-000100`, showing its latest plan `ENTERPRISE` and `_is_deleted = false`.
- **INTERVIEW QUESTION**: What is the difference between a Current-State table and an SCD Type 2 table?
- **EXPECTED ANSWER**: A current-state table maintains only the latest known after-image per business key, updated in place via upsert/merge. An SCD Type 2 table maintains the complete historical timeline of every state transition per business key by inserting new version rows with valid-from and valid-to date ranges.

---

### 2.5. Full After-Image Contract
- **WHAT IT IS**: An architectural contract specifying that every incoming `INSERT` and `UPDATE` CDC payload contains the complete state of the record, not just the modified fields (patches/deltas).
- **WHY IT MATTERS**: Full after-images allow collapsing multiple changes for the same entity within a single batch. If an account is updated three times in a batch, only the highest-sequence after-image needs to be merged into the target table.
- **WHERE USED**: Schema definitions and payload parsing.
- **FILE/FUNCTION**: [src/delta/payload_parser.py](file:///Users/vijjureddy/Library/CloudStorage/GoogleDrive-mvsreddymorthala@gmail.com/My%20Drive/Job%20Switch%20Projects/Incremental%20CDC%20Databricks%20Pipeline/src/delta/payload_parser.py#L34-L72).
- **EXAMPLE**: An `UPDATE` event on `subscriptions` includes `account_id`, `plan`, `status`, `monthly_amount`, and `renewal_date`.
- **INTERVIEW QUESTION**: Why is a full after-image contract critical when collapsing CDC events before a Delta MERGE?
- **EXPECTED ANSWER**: If CDC payloads were sparse delta patches (only containing changed fields), collapsing intermediate updates would lose the field changes made in earlier events. With full after-images, the highest-sequence event contains all current attributes, allowing safe discard of intermediate in-batch mutations.

---

### 2.6. Delta MERGE & Upsert
- **WHAT IT IS**: The `DeltaTable.merge()` operation that combines conditional `whenMatchedUpdate()`, `whenMatchedDelete()`, and `whenNotMatchedInsert()` clauses in a single atomic statement.
- **WHY IT MATTERS**: It allows declarative, atomic synchronization between incoming CDC changes and target tables without manual staging table joins or separate insert/update queries.
- **WHERE USED**: Mutation execution for current-state tables and audit ledgers.
- **FILE/FUNCTION**: [src/delta/merge.py:apply_delta_merge](file:///Users/vijjureddy/Library/CloudStorage/GoogleDrive-mvsreddymorthala@gmail.com/My%20Drive/Job%20Switch%20Projects/Incremental%20CDC%20Databricks%20Pipeline/src/delta/merge.py#L125-L210).
- **EXAMPLE**:
```python
delta_table.alias("target").merge(
    source_df.alias("source"), "target.subscription_id = source.subscription_id"
).whenMatchedUpdate(
    condition="source.source_sequence > target._last_source_sequence AND source.operation IN ('INSERT', 'UPDATE')",
    set={...},
).whenNotMatchedInsert(condition="source.operation IN ('INSERT', 'UPDATE')", values={...}).execute()
```
- **INTERVIEW QUESTION**: What causes the Delta error "Cannot perform MERGE as multiple source rows matched the same target row"?
- **EXPECTED ANSWER**: Delta MERGE requires deterministic 1-to-1 matching between incoming source rows and target rows. If the source DataFrame contains two or more rows with the same join key (e.g. two updates to the same `subscription_id`), Delta cannot determine which source row should update the target, throwing a runtime error. This must be prevented by deduplicating and collapsing source records before executing the MERGE.

---

### 2.7. Soft Delete & Tombstone Semantics
- **WHAT IT IS**: Representing a deleted record by updating its `_is_deleted` flag to `true` and advancing its `_last_source_sequence`, rather than physically deleting the row with `DELETE FROM`.
- **WHY IT MATTERS**: Physical deletes erase sequence memory. If a pipeline crashes after mutating the target table but before writing the audit ledger or checkpoint, a subsequent retry would see no target row and could erroneously re-insert an older stale change or fail crash recovery. Tombstones retain sequence evidence.
- **WHERE USED**: Target table delete mutations and active view filters.
- **FILE/FUNCTION**: [src/delta/merge.py](file:///Users/vijjureddy/Library/CloudStorage/GoogleDrive-mvsreddymorthala@gmail.com/My%20Drive/Job%20Switch%20Projects/Incremental%20CDC%20Databricks%20Pipeline/src/delta/merge.py#L140-L165), [src/delta/current_state.py](file:///Users/vijjureddy/Library/CloudStorage/GoogleDrive-mvsreddymorthala@gmail.com/My%20Drive/Job%20Switch%20Projects/Incremental%20CDC%20Databricks%20Pipeline/src/delta/current_state.py#L80-L115).
- **EXAMPLE**: When `SUB-00100` is deleted at sequence `850`, the row remains with `_is_deleted = true` and `_last_source_sequence = 850`. Business fields are retained for traceability.
- **INTERVIEW QUESTION**: Why are soft-delete tombstones preferred over physical deletes in incremental CDC pipelines?
- **EXPECTED ANSWER**: Soft-delete tombstones preserve the lineage metadata (`_last_source_sequence`, `_last_event_id`) of the deletion. This enables the pipeline to detect and discard out-of-order or stale updates arriving after the delete, allows graceful crash-recovery reconciliation, and permits clean re-activation if a subsequent `INSERT` arrives with a higher sequence number.

---

### 2.8. Protective Tombstones for Unknown Keys
- **WHAT IT IS**: Creating a tombstone record with `_is_deleted = true` when a `DELETE` event arrives for a business key that does not exist in the target table.
- **WHY IT MATTERS**: In distributed architectures or out-of-order ingestion, a `DELETE` event can arrive before the initial `INSERT`. By creating a protective tombstone with the delete sequence number, downstream state remembers that the key is deleted as of sequence $N$. If the older `INSERT` arrives later (with sequence $< N$), it is correctly classified as stale and discarded.
- **WHERE USED**: Unmatched delete handling in Delta MERGE.
- **FILE/FUNCTION**: [src/delta/merge.py](file:///Users/vijjureddy/Library/CloudStorage/GoogleDrive-mvsreddymorthala@gmail.com/My%20Drive/Job%20Switch%20Projects/Incremental%20CDC%20Databricks%20Pipeline/src/delta/merge.py#L170-L195).
- **EXAMPLE**: `DELETE INV-999` at sequence `600` creates a row with `invoice_id = INV-999`, `_is_deleted = true`, `_last_source_sequence = 600`, and business attributes set to `NULL`.
- **INTERVIEW QUESTION**: What happens if an incremental pipeline receives a DELETE event for a record that has never been seen in the target table?
- **EXPECTED ANSWER**: The pipeline creates a protective tombstone row with `_is_deleted = true` and records the delete's `source_sequence`. All unknown business attributes are set to NULL. If a delayed or out-of-order `INSERT` for that key arrives later with an older sequence, the sequence collision guard will recognize that `source_sequence < target._last_source_sequence` and reject the stale insert.

---

### 2.9. Source-Sequence Conflict Guard
- **WHAT IT IS**: A conditional check in the MERGE clause (`source.source_sequence > target._last_source_sequence`) that restricts mutations to strictly newer changes.
- **WHY IT MATTERS**: In distributed systems, network retries, repartitioning, or upstream replays can deliver older events after newer ones have already been applied. The guard guarantees monotonic state progression.
- **WHERE USED**: Delta MERGE `whenMatchedUpdate` conditions.
- **FILE/FUNCTION**: [src/delta/merge.py:apply_delta_merge](file:///Users/vijjureddy/Library/CloudStorage/GoogleDrive-mvsreddymorthala@gmail.com/My%20Drive/Job%20Switch%20Projects/Incremental%20CDC%20Databricks%20Pipeline/src/delta/merge.py#L145-L165).
- **EXAMPLE**: If target has sequence `500`, an incoming event with sequence `480` triggers no update and is audited as `STALE_NOOP`.
- **INTERVIEW QUESTION**: Why is comparing event timestamps insufficient for conflict resolution in CDC pipelines?
- **EXPECTED ANSWER**: Wall-clock timestamps suffer from clock skew across source database nodes and application servers, and multiple events within the same transaction often share the exact same timestamp. A monotonic transaction sequence number (such as a Postgres LSN or MySQL binlog position) provides strict, unambiguous total ordering.

---

### 2.10. Decimal Representation for Monetary Fields
- **WHAT IT IS**: Storing financial figures (`monthly_amount`, `amount`) using `DecimalType(12, 2)` instead of IEEE-754 floating point numbers (`DoubleType` or `FloatType`).
- **WHY IT MATTERS**: Floating point arithmetic causes binary rounding inaccuracies (e.g. `0.1 + 0.2 = 0.30000000000000004`), which can accumulate significant discrepancies in financial reconciliations, invoicing, and revenue reporting.
- **WHERE USED**: Target table schemas for `subscriptions`, `invoices`, and `payments`.
- **FILE/FUNCTION**: [src/delta/schemas.py](file:///Users/vijjureddy/Library/CloudStorage/GoogleDrive-mvsreddymorthala@gmail.com/My%20Drive/Job%20Switch%20Projects/Incremental%20CDC%20Databricks%20Pipeline/src/delta/schemas.py#L32-L72).
- **EXAMPLE**: `DecimalType(12, 2)` accurately represents `$149.99` with exact fixed-point precision up to $10^{10}$.
- **INTERVIEW QUESTION**: Why must financial and monetary attributes never be stored as `Float` or `Double` in a data warehouse or lakehouse?
- **EXPECTED ANSWER**: Floating point numbers use binary representations that cannot precisely represent fractional base-10 decimals like 0.01 or 0.10. Over millions of aggregated transaction records, rounding errors produce inexact sums. Fixed-point `DecimalType` guarantees exact base-10 precision essential for audit and financial compliance.
