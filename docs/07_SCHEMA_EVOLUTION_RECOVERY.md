# Module 3 Guide: Versioned Schema Evolution, Controlled Migrations & Recovery

## Overview

In real-world SaaS and transactional architectures, data structures inevitably evolve over time. New business fields are added, billing models shift, and producer applications update their change payloads.

In an incremental CDC pipeline, schema changes present a significant risk:
1. **Silent Schema Drift**: Downstream tables accumulating unexpected, untyped columns.
2. **Column Erasure**: Old CDC events overwriting or nullifying newly introduced columns.
3. **Late-Event Incompatibility**: Older V1 change events arriving days after downstream tables have migrated to Schema V2.
4. **Unsupported Schema Infiltration**: Producers publishing unvetted schemas that bypass validation.

This guide explains how this repository implements **controlled schema evolution** with explicit version registries, controlled Delta migrations, and backward-compatible historical replay.

---

## Architectural Principles

```
CDC Envelope: Stable & Fixed
┌─────────────────────────────────────────────────────────────┐
│ event_id, source_table, operation, business_key,           │
│ source_sequence, event_timestamp, schema_version, payload   │
└──────────────────────────────┬──────────────────────────────┘
                               │
            ┌──────────────────┴──────────────────┐
            ▼                                     ▼
   Schema Version 1                      Schema Version 2
┌───────────────────────────┐         ┌───────────────────────────┐
│ subscription_id (PK)      │         │ subscription_id (PK)      │
│ account_id                │         │ account_id                │
│ plan                      │         │ plan                      │
│ status                    │         │ status                    │
│ monthly_amount            │         │ monthly_amount            │
│ renewal_date              │         │ renewal_date              │
└───────────────────────────┘         │ billing_cycle   [NEW]     │
                                      │ currency        [NEW]     │
                                      └───────────────────────────┘
```

### 1. Stable Envelope vs. Versioned Business Payloads
- The **CDC Envelope** (`event_id`, `source_table`, `operation`, `business_key`, `source_sequence`, `event_timestamp`, `schema_version`, `payload`) remains immutable and fixed across versions.
- Only the **Business Payload** JSON is versioned.
- This prevents unnecessary restructuring of the pipeline's core routing, validation, deduplication, and staging layers.

### 2. Explicit Schema Version Registry
Instead of dynamically inferring schema from JSON strings or guessing types at runtime, schemas are registered in code:
- Implementation: [src/schema_evolution/registry.py](file:///Users/vijjureddy/Library/CloudStorage/GoogleDrive-mvsreddymorthala@gmail.com/My%20Drive/Job%20Switch%20Projects/Incremental%20CDC%20Databricks%20Pipeline/src/schema_evolution/registry.py#L35-L55).
- Registered Pairs:
  - `("subscriptions", 1)`: V1 Schema (6 business fields)
  - `("subscriptions", 2)`: V2 Schema (8 business fields: adds `billing_cycle`, `currency`)
  - `("accounts", 1)`: V1 Schema
  - `("invoices", 1)`: V1 Schema
  - `("payments", 1)`: V1 Schema

### 3. Strict Rejection of Unknown Schema Versions
- Any event with an unknown or unregistered schema version (e.g. `schema_version = 99`) is **strictly rejected** by the validation engine.
- It is immediately routed to the quarantine storage with reason `UNSUPPORTED_SCHEMA_VERSION`.
- Implementation: [src/cdc/validation.py](file:///Users/vijjureddy/Library/CloudStorage/GoogleDrive-mvsreddymorthala@gmail.com/My%20Drive/Job%20Switch%20Projects/Incremental%20CDC%20Databricks%20Pipeline/src/cdc/validation.py#L80-L90).

---

## Controlled Delta Schema Migration vs. Global autoMerge

### Why Global autoMerge Is Dangerous in Production
Enabling `spark.databricks.delta.schema.autoMerge.enabled = true` globally for a Spark session or pipeline introduces serious vulnerabilities:
- **Accidental Column Addition**: A typo in an incoming event payload (e.g. `billing_cycel`) instantly adds a permanent column to the production Delta table.
- **Silent Type Coercion**: A numeric field delivered as a string might convert the table column type or cause write failures downstream.
- **Loss of Schema Governance**: Data engineers lose track of when and why columns were added to the target state.

### Our Solution: Controlled, Explicit Migration
- Schema migrations are performed via dedicated, explicit migration routines:
  - [src/schema_evolution/migrations.py](file:///Users/vijjureddy/Library/CloudStorage/GoogleDrive-mvsreddymorthala@gmail.com/My%20Drive/Job%20Switch%20Projects/Incremental%20CDC%20Databricks%20Pipeline/src/schema_evolution/migrations.py#L20-L70) (`migrate_subscriptions_to_v2`).
- Uses Spark SQL `ALTER TABLE delta.`<path>` ADD COLUMNS (billing_cycle STRING, currency STRING)` or a scoped single-operation `option("mergeSchema", "true")`.
- Existing historical/current V1 rows receive `NULL` for the new columns until updated by a V2 event.
- Fully idempotent: if columns already exist, the operation returns `ALREADY_MIGRATED` with zero table mutation.

---

## Full After-Image Semantics Across Versions

A CDC event represents a "full after-image" *relative to the schema version under which it was generated*:

### V1 Event Full After-Image
- Carries full state for: `subscription_id`, `account_id`, `plan`, `status`, `monthly_amount`, `renewal_date`.
- Has no knowledge of `billing_cycle` or `currency`.

### V2 Event Full After-Image
- Carries full state for all 8 fields, including `billing_cycle` and `currency`.

### Forward Application Rules:
When applying a V1 event to an evolved V2 target table:
- Target table's existing V2 columns (`billing_cycle`, `currency`) **must not be nullified**.
- The Delta MERGE set clauses dynamically match only columns present in both the event version and the target schema:
  ```python
  available_business_cols = [
      c for c in target_cols
      if c in source_cols and c not in LINEAGE_FIELD_NAMES and c != pk_col
  ]
  ```
- Lineage column `_last_schema_version` is updated to reflect the version of the latest applied change.

---

## SCD2 History Across Schema Versions

The operational history table `subscriptions_history` supports rows from both V1 and V2 eras simultaneously:

1. **V1 History Rows**:
   - `schema_version = 1`
   - `billing_cycle = NULL`, `currency = NULL`
2. **V2 History Rows**:
   - `schema_version = 2`
   - `billing_cycle = 'ANNUAL'`, `currency = 'USD'`
3. **Late V1 Replay In Between V2 Rows**:
   - When a late V1 event (e.g. sequence 210) arrives after V2 events (e.g. sequence 220) are already committed:
   - Key-scoped reconstruction places the V1 event in sequence order `[210, 220)`.
   - The late V1 row has `NULL` for V2 columns.
   - The subsequent V2 version (`[220, NULL)`) retains its full V2 values (`billing_cycle='ANNUAL'`, `currency='USD'`).
   - Current state remains at sequence 220 with V2 attributes intact.

---

## Checkpoint Isolation & Recovery

### Ingestion Checkpoints vs. Replay
- The forward ingestion checkpoint (`state/checkpoint_state.json`) tracks batches processed by the normal forward lane.
- Late-event replay runs in an isolated historical correction lane.
- **Rule**: Replay MUST NOT modify, advance, or regress `completed_batch_ids` or `table_sequences` in `checkpoint_state.json`.

### Replay Audit & Idempotency
- Replay operations are logged in `delta/replay/replay_audit` using Delta MERGE on deterministic `replay_id` (`SHA256("late-replay:" + event_id)`).
- If a process crashes after writing historical versions but before updating the replay audit or queue status:
  - On restart, the rebuilder detects existing deterministic history versions.
  - History is replaced idempotently with identical version IDs.
  - Replay audit and queue status are updated to `APPLIED`.
