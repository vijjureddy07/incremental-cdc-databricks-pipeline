# Incremental CDC Databricks Pipeline

[![Python 3.11](https://img.shields.io/badge/Python-3.11-blue.svg)](https://www.python.org/downloads/release/python-3110/)
[![PySpark 3.5.x](https://img.shields.io/badge/PySpark-3.5.x-orange.svg)](https://spark.apache.org/)
[![Tests Passing](https://img.shields.io/badge/Tests-24%2F24%20Passing-brightgreen.svg)]()
[![Code Style: Ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)

An enterprise-grade, deterministic **Change Data Capture (CDC)** ingestion and incremental processing lakehouse foundation built with **PySpark** and **Apache Spark SQL**.

This project specifically demonstrates deep systems-level data engineering: handling out-of-order event arrival, deterministic deduplication, schema validation, quarantine routing, simulated database transaction log sequencing (LSNs), atomic checkpoints, and late-arriving event detection on a realistic B2B SaaS data model.

---

## 🏗️ Module 1 Architecture & Change Feed Lifecycle

```
[Simulated B2B SaaS OLTP Source]
(accounts, subscriptions, invoices, payments)
                 │
                 ▼
[Deterministic CDC Generator]
  • Sequential Batches (batch_id=000001, 000002...)
  • Simulated LSN (source_sequence) & Deterministic Hash (event_id)
  • Operations: INSERT, UPDATE, DELETE (Tombstones)
  • Injected Defects (Duplicates, Out-of-Order, Late Events, Corrupt JSON)
  • Batch Manifests (manifest.json)
                 │
                 ▼
[PySpark Explicit StructType Ingestion]
  (Zero inferSchema overhead; strict type safety)
                 │
                 ├──────────────────────────────────────┐
                 ▼                                      ▼
     [Validation Engine]                       [Quarantine Storage]
      - Schema checks                           - original_event
      - Operation validation                    - rejection_reason
      - Payload parsing                         - batch_id & timestamp
                 │
                 ▼
     [Deterministic Deduplication]
      - ROW_NUMBER() over (PARTITION BY event_id)
      - Distinguishes duplicate events from repeated updates to the same key
                 │
                 ▼
     [Source-Sequence Ordering]
      - Resolves: Arrival Order != Logical Source Order
      - Strictly orders by source_sequence ASC
                 │
                 ▼
     [Late-Event Classifier & HWM Evaluator]
      - Evaluates against table High-Water Mark (HWM)
      - Isolates late events (seq <= HWM) to late_events/ (never discarded)
                 │
                 ▼
     [Atomic Checkpoint Commit]
      - POSIX atomic rename (tempfile + os.replace)
      - Guarded against sequence regressions (CheckpointRegressionError)
      - Advances ONLY upon 100% processing success
                 │
                 ▼
     [Metrics Reconciliation & Spark SQL Analysis]
      - Verified Invariant: Raw == Valid Unique + Duplicates + Invalid
      - Temporary Views: v_cdc_valid, v_cdc_quarantine, v_cdc_late
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
      │       account_id (FK), plan, status, monthly_amount, renewal_date
      │          │
      └──────────┼──< [invoices]
                 │       invoice_id (PK)
                 │       account_id (FK), subscription_id (FK), amount, status, due_date
                 │          │
                 └──────────┼──< [payments]
                                   payment_id (PK)
                                   invoice_id (FK), amount, payment_status, processor_ref
```

---

## ⚡ Core Technical Principles Demonstrated

1. **Source Sequence (Simulated LSN)**:
   Every CDC mutation carries a monotonic integer `source_sequence` simulating database write-ahead log (WAL) positions. The pipeline guarantees state transitions are ordered by logical sequence, eliminating race conditions from physical arrival order jitter.
2. **Deterministic Event IDs**:
   Event IDs are SHA-256 hashes of immutable coordinates: `SHA256(source_table:business_key:source_sequence:operation)`. Multiple identical events generated from network retries collapse into the exact same ID.
3. **Duplicate Event vs Multiple Legitimate Updates**:
   Differentiates identical event retries (duplicate `event_id` -> deduplicated via `ROW_NUMBER()`) from multiple legitimate state transitions to the same entity (e.g. plan upgrades from `BASIC` to `PRO` to `ENTERPRISE` have distinct `source_sequence` values and are both preserved).
4. **Tombstone DELETE Handling**:
   DELETE events carry reduced payloads containing only primary keys and deletion metadata, validated through specialized tombstone rules.
5. **Atomic Checkpoints & Regression Guards**:
   Persistent checkpoints (`state/checkpoints/{table}.json`) use atomic file replacement (`os.replace` with `os.fsync`) and raise `CheckpointRegressionError` if sequence numbers attempt to regress backwards.
6. **Late-Arriving Events Isolation**:
   Events arriving with `source_sequence <= current_hwm` are detected and preserved in `output/late_events/` for downstream reconciliation, proving that late data is never silently dropped.

---

## 🚀 Quickstart & CLI Usage

### 1. Environment Setup
```bash
# Prerequisites: Python 3.11, OpenJDK 17
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
```

### 2. Run the End-to-End Workflow
```bash
# Generates initial snapshot, 3 CDC batches with injected defects, processes them, and runs SQL diagnostics
python -m src.main run-all --scale tiny --batches 3
```

### 3. Individual CLI Commands
```bash
# 1. Generate initial OLTP snapshot
python -m src.main generate-snapshot --scale tiny --seed 42

# 2. Generate CDC change batches
python -m src.main generate-cdc --scale tiny --batches 3 --seed 42

# 3. Incrementally process pending CDC batches
python -m src.main process-cdc

# 4. Run Spark SQL diagnostic analytics
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
| **Module 2** | **Delta MERGE + Current-State Tables + Delete Handling** | ⏳ *NOT IMPLEMENTED YET* |
| **Module 3** | **Late Events + SCD2 + Schema Evolution + Replay / Recovery** | ⏳ *NOT IMPLEMENTED YET* |
| **Module 4** | **Databricks Auto Loader Architecture + Jobs + CI + Final QA** | ⏳ *NOT IMPLEMENTED YET* |

---

## 📚 In-Depth Documentation

- [00_LEARNING_INDEX.md](docs/00_LEARNING_INDEX.md): Index of concepts and study tracking.
- [01_CDC_FOUNDATIONS.md](docs/01_CDC_FOUNDATIONS.md): Deep dive into CDC concepts, LSNs, and transaction logs.
- [02_INCREMENTAL_PROCESSING.md](docs/02_INCREMENTAL_PROCESSING.md): Full vs incremental loads, checkpoints, HWM, and streaming watermarks.
- [03_EVENT_ORDERING_DEDUP.md](docs/03_EVENT_ORDERING_DEDUP.md): Change ordering, deduplication, and late event strategies.
- [IMPLEMENTATION_MAP.md](docs/IMPLEMENTATION_MAP.md): Complete requirements to code trace matrix.
- [INTERVIEW_QA.md](docs/INTERVIEW_QA.md): Production data engineering interview questions & answers.
