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
CDC JSONL Change Feed
      │
      ▼
  Validation
      │
      ▼
Deterministic Dedupe
      │
      ▼
Sequence Ordering
      │
      ▼
Delta CDC Apply
      │
      ▼
┌─────────────────────────────┐
│ Current-State Delta Tables  │
│ accounts                    │
│ subscriptions               │
│ invoices                    │
│ payments                    │
└─────────────────────────────┘
      │
      ▼
Applied Event Ledger
      │
      ▼
Atomic Batch Checkpoint
```

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

### 3. Module 1 Diagnostic Commands
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

### 4. Running the Test Suite
```bash
pytest tests/ -v
ruff check .
```

---

## 🗺️ Engineering Roadmap & Modules

| Module | Scope | Status |
| :--- | :--- | :--- |
| **Module 1** | **CDC Source Simulation + Incremental Change Feed Foundation** | ✅ **COMPLETED (24/24 Tests)** |
| **Module 2** | **Delta MERGE + Current-State Tables + Delete Handling** | ✅ **COMPLETED (40/40 Tests)** |
| **Module 3** | **Late Replay + SCD2 + Schema Evolution** | ⏳ *NOT IMPLEMENTED YET* |
| **Module 4** | **Auto Loader + Databricks Jobs + CI** | ⏳ *NOT IMPLEMENTED YET* |

---

## 📚 In-Depth Documentation

- [PROGRESS.md](docs/PROGRESS.md): Factual progress tracking across Build, Test, and Learning statuses.
- [00_LEARNING_INDEX.md](docs/00_LEARNING_INDEX.md): Index of concepts and study tracking.
- [01_CDC_FOUNDATIONS.md](docs/01_CDC_FOUNDATIONS.md): Deep dive into CDC concepts, LSNs, and transaction logs.
- [02_INCREMENTAL_PROCESSING.md](docs/02_INCREMENTAL_PROCESSING.md): Full vs incremental loads, checkpoints, HWM, and streaming watermarks.
- [03_EVENT_ORDERING_DEDUP.md](docs/03_EVENT_ORDERING_DEDUP.md): Change ordering, deduplication, and late event strategies.
- [04_DELTA_CURRENT_STATE.md](docs/04_DELTA_CURRENT_STATE.md): Delta Lake storage layer, schemas, MERGE upsert, and soft-delete tombstones.
- [05_CDC_MERGE_IDEMPOTENCY.md](docs/05_CDC_MERGE_IDEMPOTENCY.md): Idempotent processing, crash recovery, within-batch collapsing, and atomic checkpoints.
- [IMPLEMENTATION_MAP.md](docs/IMPLEMENTATION_MAP.md): Complete requirements to code trace matrix.
- [INTERVIEW_QA.md](docs/INTERVIEW_QA.md): 25 production data engineering interview questions & answers.
