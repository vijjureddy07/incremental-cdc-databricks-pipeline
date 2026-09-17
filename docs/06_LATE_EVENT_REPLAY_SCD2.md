# Module 3 Guide: Late-Event Replay, Operational SCD2 History & Invariant Verification

## Overview

In Change Data Capture (CDC) architectures, distributed networks and out-of-order delivery frequently cause mutations generated earlier at the OLTP source database to arrive downstream *after* newer states have already been published.

This guide details the design, mathematical validity invariants, and key-scoped reconstruction mechanics implemented in Module 3 to handle late-arriving events without corrupting current business state or regressing incremental ingestion checkpoints.

---

## Core Concepts & Architectural Patterns

### 1. Late-Arriving CDC Event
* **What It Is**: An event whose logical source transaction order (`source_sequence`) precedes the sequence number already reflected in downstream state (`target._last_source_sequence`).
* **Why It Matters**: If applied naively using standard last-write-wins or arrival-order updates, an older state would overwrite a newer state, causing silent data corruption and incorrect billing/subscription status.
* **Where Used**: Ingestion classification & replay queue detection.
* **Exact File/Function**: [src/replay/queue.py](file:///Users/vijjureddy/Library/CloudStorage/GoogleDrive-mvsreddymorthala@gmail.com/My%20Drive/Job%20Switch%20Projects/Incremental%20CDC%20Databricks%20Pipeline/src/replay/queue.py#L65-L105) (`enqueue_late_events`), [src/delta/merge.py](file:///Users/vijjureddy/Library/CloudStorage/GoogleDrive-mvsreddymorthala@gmail.com/My%20Drive/Job%20Switch%20Projects/Incremental%20CDC%20Databricks%20Pipeline/src/delta/merge.py#L115-L180).
* **Example**:
  - Current physical table state: `SUB-000001` at sequence 150 (`status='ACTIVE'`, `plan='ENTERPRISE'`).
  - Late-arriving event: `SUB-000001` at sequence 135 (`status='PAUSED'`, `plan='PRO'`).
  - Result: Current state remains at sequence 150. The late change is routed to historical replay.

### 2. Source Order vs. Arrival Order
* **What It Is**: Source order represents the authoritative linear position in the database WAL / transaction log (`source_sequence`). Arrival order is the non-deterministic physical arrival timestamp in cloud storage or message brokers.
* **Why It Matters**: Networks have variable latency, retries, and multi-partition routing. Arrival order can never be trusted as the timeline contract.
* **Where Used**: Throughout sorting, deduplication, and replay logic.
* **Exact File/Function**: [src/replay/history_rebuilder.py](file:///Users/vijjureddy/Library/CloudStorage/GoogleDrive-mvsreddymorthala@gmail.com/My%20Drive/Job%20Switch%20Projects/Incremental%20CDC%20Databricks%20Pipeline/src/replay/history_rebuilder.py#L110-L125).

### 3. Operational SCD Type 2 vs. Kimball Dimensional History
* **What It Is**: An operational SCD2 table records the exact state evolution of a source-system entity indexed by source sequence (`[valid_from_sequence, valid_to_sequence)`). A dimensional warehouse SCD2 uses surrogate keys, date dimensions, and business effective dates for OLAP star schema joins.
* **Why It Matters**: Operational history provides auditable, reconstructible system-of-record state transitions. It answers: *"What was the exact source database state between transaction log offsets 120 and 135?"*
* **Where Used**: `delta/history/subscriptions_history`.
* **Exact File/Function**: [src/history/subscriptions_scd2.py](file:///Users/vijjureddy/Library/CloudStorage/GoogleDrive-mvsreddymorthala@gmail.com/My%20Drive/Job%20Switch%20Projects/Incremental%20CDC%20Databricks%20Pipeline/src/history/subscriptions_scd2.py#L45-L75).

### 4. Sequence-Based Validity Intervals (`valid_from_sequence` & `valid_to_sequence`)
* **What It Is**: Half-open intervals `[valid_from_sequence, valid_to_sequence)`.
  - For historical versions: `valid_to_sequence` equals the `valid_from_sequence` of the immediate successor version.
  - For the active current version: `valid_to_sequence IS NULL` and `is_current = true`.
* **Why It Matters**: Eliminates ambiguities caused by wall-clock clock drift, microsecond timestamp collisions, and timezone discrepancies.
* **Where Used**: History table schema and validation engine.
* **Exact File/Function**: [src/history/subscriptions_scd2.py](file:///Users/vijjureddy/Library/CloudStorage/GoogleDrive-mvsreddymorthala@gmail.com/My%20Drive/Job%20Switch%20Projects/Incremental%20CDC%20Databricks%20Pipeline/src/history/subscriptions_scd2.py#L170-L240) (`validate_subscription_history_invariants`).

### 5. Key-Scoped Timeline Rebuild
* **What It Is**: Rather than attempting fragile, multi-row interval patching across an entire Delta table, the rebuilder isolates the single affected business key (`subscription_id`), loads its baseline and all canonical events from `cdc_event_store`, reconstructs its timeline in memory, verifies invariants, and atomically overwrites only that key's history partitions/rows.
* **Why It Matters**: Eliminates table-wide lock contention, simplifies concurrency, guarantees mathematical contiguity, and ensures 100% deterministic, idempotent reruns.
* **Where Used**: Replay execution.
* **Exact File/Function**: [src/replay/history_rebuilder.py](file:///Users/vijjureddy/Library/CloudStorage/GoogleDrive-mvsreddymorthala@gmail.com/My%20Drive/Job%20Switch%20Projects/Incremental%20CDC%20Databricks%20Pipeline/src/replay/history_rebuilder.py#L40-L280) (`reconstruct_subscription_timeline_for_key`).

### 6. Checkpoint Isolation
* **What It Is**: Replay executes strictly in a *correction lane*. It reads historical events and updates history/audit tables, but NEVER modifies or regresses the global forward ingestion checkpoint (`checkpoint_state.json`).
* **Why It Matters**: Regressing a global ingestion high-water mark would cause forward streams to re-ingest and re-merge already committed batches, breaking pipeline idempotency.
* **Where Used**: [src/replay/processor.py](file:///Users/vijjureddy/Library/CloudStorage/GoogleDrive-mvsreddymorthala@gmail.com/My%20Drive/Job%20Switch%20Projects/Incremental%20CDC%20Databricks%20Pipeline/src/replay/processor.py#L40-L130).

---

## Concrete Replay Scenario & Timeline Correction

### Scenario Demonstration: `SUB-REPLAY-001`

1. **Initial Snapshot Baseline**:
   - `sequence = 100`, `plan = 'BASIC'`, `status = 'ACTIVE'`
   - History: Version 1 `[100, NULL)`, `is_current = true`
   - Current State: `sequence = 100`, `plan = 'BASIC'`

2. **Normal Forward CDC**:
   - Event at `sequence = 120`: `plan = 'PRO'`
   - Event at `sequence = 150`: `plan = 'ENTERPRISE'`
   - History:
     - Version 1: `[100, 120)`, `is_current = false`, `plan = 'BASIC'`
     - Version 2: `[120, 150)`, `is_current = false`, `plan = 'PRO'`
     - Version 3: `[150, NULL)`, `is_current = true`, `plan = 'ENTERPRISE'`
   - Current State: `_last_source_sequence = 150`, `plan = 'ENTERPRISE'`

3. **Late-Arriving Unseen Event**:
   - Physically arrives in Batch 5 with `sequence = 135`, `status = 'PAUSED'`, `plan = 'PRO'`.
   - Forward merge classifier sees: `135 < 150` -> `STALE_NOOP` on current state.
   - Replay queue enqueues event as `PENDING`.

4. **Replay Execution & Historical Repair**:
   - Rebuilder pulls baseline (100) and events (120, 135, 150) from `cdc_event_store`.
   - Sorts strictly: 100 -> 120 -> 135 -> 150.
   - Repaired Timeline:
     - Version 1: `[100, 120)`, `plan = 'BASIC'`
     - Version 2: `[120, 135)`, `plan = 'PRO'`, `valid_to` repaired from 150 to 135!
     - Version 3: `[135, 150)`, `plan = 'PRO'`, `status = 'PAUSED'` (NEW late version)
     - Version 4: `[150, NULL)`, `plan = 'ENTERPRISE'`, `is_current = true`
   - Current State Verification: Highest history sequence (150) matches current state (150). Current state row is NOT mutated.
   - Replay Audit commits with status `APPLIED`. Queue item updated to `APPLIED`.

---

## The 11 SCD2 History Invariants

Every reconstructed timeline is validated against the following 11 mathematical invariants prior to committing to Delta storage:

| # | Invariant Rule | Failure Exception |
|---|----------------|-------------------|
| 1 | `history_version_id` is globally unique for every row | `HistoryInvariantError` |
| 2 | `valid_from_sequence` is unique within each entity timeline | `HistoryInvariantError` |
| 3 | Versions are ordered strictly ascending by `valid_from_sequence` | `HistoryInvariantError` |
| 4 | Every non-current row must have `valid_to_sequence > valid_from_sequence` | `HistoryInvariantError` |
| 5 | Adjacent rows are contiguous: `previous.valid_to_sequence == next.valid_from_sequence` | `HistoryInvariantError` |
| 6 | Maximum one version per key has `is_current = true` | `HistoryInvariantError` |
| 7 | Exactly one version per represented key has `is_current = true` (including deletion tombstones) | `HistoryInvariantError` |
| 8 | The current version has `valid_to_sequence IS NULL` and `valid_to_event_timestamp IS NULL` | `HistoryInvariantError` |
| 9 | Highest history sequence matches current-state table `_last_source_sequence` | `CurrentStateDriftError` |
| 10 | Current history `is_deleted` matches current-state table `_is_deleted` | `CurrentStateDriftError` |
| 11 | Current non-deleted business attributes match current-state table attributes | `CurrentStateDriftError` |

---

## Failure Recovery & Sequence Collisions

### Sequence Collision Handling
If two distinct `event_id` values share the exact same `(source_table, business_key, source_sequence)`:
- In a valid linear database WAL, an entity cannot undergo two independent mutations at the identical transaction sequence.
- Replay halts for this key and raises `SourceSequenceConflictError`.
- Replay queue entry is marked `CONFLICT` with the exact conflicting event IDs recorded in `failure_reason`.
- History is preserved unchanged without guessing or arbitrary tie-breaking.

### Replay Crash Recovery
Because Delta Lake transactions are atomic per table, a crash could occur after history table commit but before replay queue or audit status commits.
- **On Retry**: The replay processor re-evaluates the key from the event store.
- Because history version IDs are deterministic hashes (`SHA256(sub_id + seq + event_id)`), re-running generates the identical DataFrame and overwrites the key's history idempotently without duplicating rows.
- The audit record is upserted via Delta MERGE on `replay_id` (`SHA256("late-replay:" + event_id)`), and queue status is set to `APPLIED`.
