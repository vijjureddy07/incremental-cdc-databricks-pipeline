# Incremental CDC Databricks Pipeline

[![Python 3.11](https://img.shields.io/badge/Python-3.11-blue.svg)](https://www.python.org/downloads/release/python-3110/)
[![PySpark 3.5.x](https://img.shields.io/badge/PySpark-3.5.x-orange.svg)](https://spark.apache.org/)
[![Delta Lake 3.2.x](https://img.shields.io/badge/Delta%20Lake-3.2.x-blue.svg)](https://delta.io/)
[![Code Style: Ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)

A focused CDC portfolio implementation and local CDC processing pipeline built with **PySpark**, **Delta Lake**, and **Apache Spark SQL**. Demonstrates how operational OLTP change feeds mutate downstream tables safely through sequence ordering, deterministic deduplication, Delta MERGE, soft-delete tombstones, applied event auditing, crash recovery, and atomic batch checkpoints on a realistic B2B SaaS data model.

---

## 🏗️ End-to-End Pipeline Architecture

```
Simulated OLTP
      │
      ▼
CDC Batches
      │
      ▼
Validation + Dedupe
      │
      ▼
Canonical Event Store (delta/events/cdc_event_store)
      │
      ▼
Normal CDC Apply
      ├──────────────────────────────────────────────┐
      ▼                                              ▼
Current-State Delta Tables               Subscription SCD2 History
(accounts, subscriptions, ...)           (delta/history/subscriptions_history)
      ▲                                              ▲
      │                                              │
Late Event Detection                                 │
      │                                              │
      ▼                                              │
Replay Queue (delta/replay/late_event_queue)         │
      │                                              │
      ▼                                              │
Key-Scoped Replay ───────────────────────────────────┘
      │
      ▼
Replay Audit (delta/replay/replay_audit)
```

### Versioned Schema Registry
* **Schema V1**: `subscription_id`, `account_id`, `plan`, `status`, `monthly_amount`, `renewal_date`
* **Schema V2**: Adds `billing_cycle` (`MONTHLY`, `ANNUAL`) and `currency` (`USD`, `EUR`, `GBP`, `INR`)


---

## 📊 Domain Data Model (B2B SaaS Platform)

The project models a high-throughput subscription SaaS engine:

```
[accounts]
   account_id (PK)
   company_name, industry, country, plan_tier
      │
      ├──< [subscriptions]
      │       subscription_id (PK)
      │       account_id (FK), plan, status, monthly_amount (Decimal), renewal_date
      │          │
      └──────────┼──< [invoices]
                 │       invoice_id (PK)
                 │       account_id (FK), subscription_id (FK), amount (Decimal), status, due_date
                 │          │
                 └──────────┼──< [payments]
                                   payment_id (PK)
                                   invoice_id (FK), amount (Decimal), payment_status, processor_ref
```

All monetary fields (`monthly_amount`, `amount`) are strictly defined as `DecimalType(12, 2)` to eliminate floating-point rounding inaccuracies in financial reconciliation.

---

## ⚡ Core Technical Principles Demonstrated

1. **Source Sequence (Simulated LSN)**:
   Every CDC mutation carries a monotonic integer `source_sequence` simulating database write-ahead log (WAL/binlog) positions, ensuring state transitions are applied in exact commit order.
2. **Deterministic Event IDs & Stable Tie-Breaking**:
   Event IDs are SHA-256 hashes of immutable coordinates: `SHA256(source_table:business_key:source_sequence:operation)`. Full-row tie-breaking uses canonical event hashing to guarantee 100% deterministic duplicate selection across distributed workers.
3. **Full After-Image Contract & In-Batch Collapsing**:
   INSERT and UPDATE payloads provide complete after-images. When multiple updates to the same business key arrive in a single batch, the pipeline selects the winning after-image via `ROW_NUMBER()`, preventing ambiguous multi-match Delta MERGE errors while auditing earlier events as `SUPERSEDED_WITHIN_BATCH`.
4. **Delta MERGE & Soft-Delete Tombstones**:
   Deletions retain tombstone rows (`_is_deleted = true`, advancing `_last_source_sequence`). This retains sequence memory on the target table, protecting against older out-of-order updates and enabling graceful re-insert reactivation if a higher-sequence INSERT arrives later.
5. **Applied Event Ledger & Crash Recovery**:
   Audit records are persisted to `delta/audit/applied_events` via Delta MERGE on `event_id`. If a pipeline run crashes after mutating the target table but before writing audit logs, retry logic detects target state, avoids duplicate mutation, and repairs the ledger with outcome `RECOVERED_AFTER_PARTIAL_COMMIT`.
6. **Batch-Atomic Global Checkpoints**:
   A single unified state file (`state/checkpoints/checkpoint_state.json`) advances high-water marks across all 4 domain tables simultaneously using POSIX atomic renames (`os.replace` + `os.fsync`) only after all Delta table writes succeed.

---

## 🚀 Quickstart & CLI Usage

### 1. Environment Setup
```bash
# Prerequisites: Python 3.11, OpenJDK 17
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
```

### 2. Run Module 2 Delta Workflow
```bash
# 1. Initialize local Delta current-state tables from snapshot
python -m src.main init-delta --scale tiny

# 2. Apply pending CDC batches to Delta tables with MERGE
python -m src.main apply-cdc

# 3. Inspect current-state tables
python -m src.main show-current --table subscriptions

# 4. View applied-event audit ledger
python -m src.main show-applied-events
```

### 3. Run Module 3 Late-Event Replay, SCD2 & Schema Evolution Workflow
```bash
# 1. Backfill canonical Delta event store from generated CDC batches
python -m src.main build-event-store

# 2. Initialize operational SCD2 subscription history from snapshot
python -m src.main init-history --scale tiny

# 3. Detect late-arriving events and stage to replay queue
python -m src.main detect-late-events

# 4. Replay late events with deterministic key-scoped historical rebuild
python -m src.main replay-late-events

# 5. Inspect subscription SCD2 history for a specific business key
python -m src.main show-history --subscription-id SUB-000001

# 6. View late-event replay queue status
python -m src.main show-replay-queue

# 7. Perform controlled schema migration to Subscription V2
python -m src.main migrate-schema --table subscriptions --to-version 2
```

### 4. Module 1 Diagnostic Commands
```bash
# Generate initial OLTP snapshot
python -m src.main generate-snapshot --scale tiny --seed 42

# Generate CDC change batches with injected defects
python -m src.main generate-cdc --scale tiny --batches 3 --seed 42

# Incrementally validate and stage pending CDC batches
python -m src.main process-cdc

# Run Spark SQL analytical diagnostics
python -m src.main analyze-cdc
```

### 5. Running the Test Suite
```bash
pytest tests/ -v
ruff check .
```

---

## 🗺️ Engineering Roadmap & Modules

| Module | Scope | Status |
| :--- | :--- | :--- |
| **Module 1** | **CDC Source Simulation + Incremental Change Feed Foundation** | ✅ **COMPLETED (24 pytest items passing)** |
| **Module 2** | **Delta MERGE + Current-State Tables + Delete Handling** | ✅ **COMPLETED (18 pytest items passing / 40 requirements covered)** |
| **Module 3** | **Late Replay + SCD2 History + Schema Evolution** | ✅ **COMPLETED (15 pytest items passing / all requirements verified)** |
| **Module 4** | **Databricks Auto Loader + Jobs + CI/CD** | ⏳ *NOT IMPLEMENTED YET* |

> **Global Regression Metric**: Exactly **57 pytest items collected and passing** (`pytest --collect-only -q` -> 57 items; `57 passed, 0 failed` in `pytest -v`).

---

## 📚 In-Depth Documentation

- [PROGRESS.md](docs/PROGRESS.md): Factual progress tracking across Build, Test, and Learning statuses.
- [00_LEARNING_INDEX.md](docs/00_LEARNING_INDEX.md): Index of concepts and study tracking.
- [01_CDC_FOUNDATIONS.md](docs/01_CDC_FOUNDATIONS.md): Deep dive into CDC concepts, LSNs, and transaction logs.
- [02_INCREMENTAL_PROCESSING.md](docs/02_INCREMENTAL_PROCESSING.md): Full vs incremental loads, checkpoints, HWM, and streaming watermarks.
- [03_EVENT_ORDERING_DEDUP.md](docs/03_EVENT_ORDERING_DEDUP.md): Change ordering, deduplication, and late event strategies.
- [04_DELTA_CURRENT_STATE.md](docs/04_DELTA_CURRENT_STATE.md): Delta Lake storage layer, schemas, MERGE upsert, and soft-delete tombstones.
- [05_CDC_MERGE_IDEMPOTENCY.md](docs/05_CDC_MERGE_IDEMPOTENCY.md): Idempotent processing, crash recovery, within-batch collapsing, and atomic checkpoints.
- [06_LATE_EVENT_REPLAY_SCD2.md](docs/06_LATE_EVENT_REPLAY_SCD2.md): Late-arriving CDC events, arrival vs source order, canonical event store, replay queue, operational SCD2 history, key-scoped timeline rebuild, collision handling, and replay idempotency.
- [07_SCHEMA_EVOLUTION_RECOVERY.md](docs/07_SCHEMA_EVOLUTION_RECOVERY.md): Versioned payload schemas, controlled Delta schema evolution, why global autoMerge is risky, unknown schema quarantine, backward compatibility, and checkpoint isolation.
- [IMPLEMENTATION_MAP.md](docs/IMPLEMENTATION_MAP.md): Complete requirements to code trace matrix.
- [INTERVIEW_QA.md](docs/INTERVIEW_QA.md): 41 production data engineering interview questions & answers.

