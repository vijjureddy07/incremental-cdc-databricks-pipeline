# Project Progress Tracker

This document tracks the factual build, test, and learning status of all modules in the `incremental-cdc-databricks-pipeline` repository.

> [!NOTE]
> Learning statuses strictly reflect pedagogical study status and remain **NOT STUDIED / PENDING** during active development until explicit review sessions.

---

## Status Summary

| Module | Focus Area | Build Status | Test Status | Learning Status |
| :--- | :--- | :--- | :--- | :--- |
| **Module 1** | CDC Source Simulation & Change Feed Foundation | **BUILD COMPLETE** | **TESTS PASS** (24/24) | **NOT STUDIED / PENDING** |
| **Module 2** | Delta MERGE, Current-State Tables & Delete Handling | **BUILD COMPLETE** | **TESTS PASS** (40/40) | **NOT STUDIED / PENDING** |
| **Module 3** | Late Events, SCD2 History & Schema Evolution | **NOT STARTED** | **NOT STARTED** | **NOT STUDIED / PENDING** |
| **Module 4** | Databricks Auto Loader, Jobs & CI/CD | **NOT STARTED** | **NOT STARTED** | **NOT STUDIED / PENDING** |

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
  - 40 new automated unit and integration tests (64 total tests passing).

### Module 3: Late Events + SCD2 + Schema Evolution + Replay / Recovery
- **Status**: NOT STARTED

### Module 4: Databricks Auto Loader Architecture + Jobs + CI + Final QA
- **Status**: NOT STARTED
