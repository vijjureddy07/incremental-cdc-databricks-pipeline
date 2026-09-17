# Project Progress Tracker

This document tracks the factual build, test, and learning status of all modules in the `incremental-cdc-databricks-pipeline` repository.

> [!NOTE]
> Learning statuses strictly reflect pedagogical study status and remain **NOT STUDIED / PENDING** during active development until explicit review sessions.

---

## Status Summary

| Module | Focus Area | Build Status | Test Status | Learning Status |
| :--- | :--- | :--- | :--- | :--- |
| **Module 1** | CDC Source Simulation & Change Feed Foundation | **BUILD COMPLETE** | **24 pytest items passing** | **NOT STUDIED / PENDING** |
| **Module 2** | Delta MERGE, Current-State Tables & Delete Handling | **BUILD COMPLETE** | **18 pytest items passing**<br>(40 Module 2 verification requirements covered) | **NOT STUDIED / PENDING** |
| **Module 3** | Late Events, SCD2 History & Schema Evolution | **BUILD COMPLETE**<br>*(LOCAL VERIFIED)* | **15 pytest items passing**<br>(All Module 3 verification requirements covered) | **NOT STUDIED / PENDING** |
| **Module 4** | Databricks Auto Loader, Jobs & CI/CD | **NOT STARTED** | **NOT STARTED** | **NOT STUDIED / PENDING** |

> **Global Regression Metric**: Exactly **57 pytest items collected and passing** (`57 passed, 0 failed` across the entire test suite).

---

## Detailed Milestone Log

### Module 1: CDC Source Simulation & Incremental Change Feed Foundation
- **Baseline Commit**: `e48381a1499fef3c0d83c191d60f5278595499fc`
- **Scope Completed**:
  - Deterministic B2B SaaS initial snapshot generator (`accounts`, `subscriptions`, `invoices`, `payments`).
  - CDC batch generator simulating sequential OLTP change feeds (`INSERT`, `UPDATE`, `DELETE`).
  - Explicit PySpark StructType schema enforcement for CDC envelope.
  - Deterministic SHA-256 `event_id` generation.
  - Simulated `source_sequence` (representing transactional WAL/binlog ordering).
  - Validation engine with quarantine routing and detailed rejection reasons.
  - Deterministic event deduplication via `ROW_NUMBER()`.
  - In-batch source-sequence ordering (`arrival order != source order`).
  - High-water mark tracking and atomic checkpoint persistence.
  - Late-event detection against table high-water marks.
  - Spark SQL diagnostic analytical views.
  - 24 automated unit and integration tests passing.

### Module 2: Delta MERGE + Current-State Tables + Delete Handling
- **Scope Completed**:
  - Module 1 hardening: deterministic duplicate tie-breaker (`canonical_event_hash`), event-id integrity verification (`EVENT_ID_MISMATCH`), payload PK consistency (`PAYLOAD_KEY_MISMATCH`), and atomic global batch checkpoints (`checkpoint_state.json`).
  - Local Delta Lake integration (`delta-spark 3.2.1` with Apache Spark 3.5.9).
  - Current-state Delta table initialization from snapshot with lineage metadata.
  - Explicit target schemas with `DecimalType(12, 2)` monetary representations.
  - PySpark built-in `from_json` table-specific payload parsing.
  - Within-batch mutation collapsing to winning `source_sequence` (`SUPERSEDED_WITHIN_BATCH` audit classification).
  - Soft-delete tombstone semantics (`_is_deleted = true`, retaining historical attributes).
  - Protective tombstone insertion for unknown-key DELETE events.
  - Re-insert after delete reactivates record with new after-image.
  - Source-sequence collision guards (`source_sequence > target._last_source_sequence`).
  - Stale change isolation (`STALE_NOOP`).
  - Idempotent Applied Event Ledger (`delta/audit/applied_events`) merged by `event_id`.
  - Crash recovery for target/ledger gap (`RECOVERED_AFTER_PARTIAL_COMMIT`).
  - Delete crash recovery preserving tombstone state.
  - Current-state invariant reconciliation checks.
  - 18 automated unit and integration test functions covering 40 Module 2 verification requirements.

### Module 3: Late Events + SCD2 + Schema Evolution + Replay / Recovery
- **Status**: **BUILD COMPLETE** *(LOCAL VERIFIED)*
- **Learning Status**: **NOT STUDIED / PENDING**
- **Scope Completed**:
  - **Part A: Module 2 Reliability & Scalability Hardening**:
    - Replaced driver-side candidate primary key `.collect()` with distributed Spark `LEFT JOIN` against target metadata projection (`_last_source_sequence`, `_last_event_id`, `_is_deleted`, `_last_schema_version`). Severed Catalyst lineage using `localCheckpoint(eager=True)`.
    - Eliminated whole-ledger driver collection by using distributed joins against `applied_events` to classify `ALREADY_APPLIED` vs `RECOVERED_AFTER_PARTIAL_COMMIT`.
    - Replaced union-like legacy checkpoint migration with global-safe `INTERSECTION` across all 4 required tables (`accounts`, `subscriptions`, `invoices`, `payments`).
    - Added fail-loud `DeltaStateError` when encountering corrupt or unreadable existing Delta paths instead of silently overwriting.
    - Factual pytest item reporting: corrected documentation to reflect actual collected test counts (57 items).
  - **Part B: Canonical CDC Event Store (`delta/events/cdc_event_store/`)**:
    - Durable 14-field schema storing all valid deduplicated CDC events prior to current-state collapsing.
    - Idempotent Delta MERGE on `event_id` with safe audit updates (`last_seen_at`) and `EventIdentityConflictError` on conflicting immutable payload content.
    - Deterministic backfill function rebuilding event store from raw CDC batches.
  - **Part C: Late Event Replay Queue (`delta/replay/late_event_queue/`)**:
    - Durable queue managing late-arriving mutations (`PENDING`, `APPLIED`, `NOOP`, `CONFLICT`, `FAILED`).
    - Strict classification: distinguishes duplicate re-deliveries from genuine late unseen logical mutations.
    - Replay Checkpoint Isolation: late replay never regresses or modifies the global forward batch ingestion checkpoint (`checkpoint_state.json`).
  - **Part D: Operational SCD2 History (`delta/history/subscriptions_history/`)**:
    - Source-system state history table tracking sequence-ordered state intervals `[valid_from_sequence, valid_to_sequence)`.
    - Seeded from initial snapshot using `SNAPSHOT_INIT` lineage markers.
    - Deterministic `history_version_id` via `SHA256(subscription_id + valid_from_sequence + source_event_id)`.
    - Full after-image forward progression: closes previous version, opens new active version; creates deletion tombstone on DELETE; reactivates on re-insert.
  - **Part E: Late Event History Reconstruction**:
    - Key-scoped rebuild: isolates affected business key, loads snapshot baseline and full event store history, sorts by `(source_sequence, event_id)`.
    - Sequence collision detection: raises `SourceSequenceConflictError` and routes queue item to `CONFLICT` without mutating history if duplicate sequences carry differing event IDs.
    - Contiguous interval repair: inserts late version between existing versions, adjusts predecessor `valid_to_sequence`, sets late version `valid_to_sequence` to successor `valid_from_sequence`.
    - Current-state drift verification: raises `CurrentStateDriftError` if latest historical version does not align with current-state Delta table.
    - Enforces 11 temporal/sequence invariants via `validate_subscription_history_invariants()`.
  - **Part F: Replay Audit Ledger & Crash Recovery (`delta/replay/replay_audit/`)**:
    - Audit ledger tracking before/after versions, sequences, and state changes via deterministic `replay_id` (`SHA256("late-replay:" + event_id)`).
    - Crash recovery: detects already-committed history rebuilds on retry and safely completes queue/audit tracking without duplicate versions.
  - **Part G: Schema Version Registry & Controlled Evolution**:
    - Version-aware payload parsing via `PAYLOAD_SCHEMA_REGISTRY` mapping `(source_table, schema_version)` to explicit PySpark schemas.
    - Subscription Schema V2: added `billing_cycle` (`MONTHLY`, `ANNUAL`) and `currency` (`USD`, `EUR`, `GBP`, `INR`).
    - Controlled Delta schema migration: explicit column addition without globally enabling dangerous `autoMerge`.
    - Lineage tracking: added `_last_schema_version` across current-state tables.
    - Backward compatibility: V1 events applied to V2 target preserve V2 columns without nullification.
    - Schema validation: unknown versions (e.g., v99) are quarantined with `UNSUPPORTED_SCHEMA_VERSION`.
  - **Part H-N: CLI, Demonstrations & Verification**:
    - Extended CLI with 7 subcommands (`build-event-store`, `init-history`, `detect-late-events`, `replay-late-events`, `show-history`, `show-replay-queue`, `migrate-schema`).
    - Deterministic demonstration scenarios for late replay (`SUB-REPLAY-001`) and schema evolution (`SUB-EVOLVE-001`).
    - 15 new automated test items (57 total pytest items passing across the test suite).

### Module 4: Databricks Auto Loader Architecture + Jobs + CI + Final QA
- **Status**: NOT STARTED

