# Learning Index: Incremental CDC Databricks Pipeline

> [!IMPORTANT]
> **Workflow Policy**: BUILD FIRST → DOCUMENT EVERYTHING → LEARN LATER.
> All learning statuses remain **NOT STUDIED / PENDING** until explicitly reviewed with the user. No quizzing.

---

## Module Progress & Learning Status

| Module | Focus Area | Build Status | Test Status | Learning Status |
| :--- | :--- | :--- | :--- | :--- |
| **Module 1** | **CDC Source Simulation + Incremental Change Feed Foundation** | **COMPLETED** | **ALL PASSING (24/24)** | **NOT STUDIED / PENDING** |
| **Module 2** | Delta MERGE + Current-State Tables + Delete Handling | NOT STARTED | NOT STARTED | NOT STUDIED / PENDING |
| **Module 3** | Late Events + SCD2 + Schema Evolution + Replay / Recovery | NOT STARTED | NOT STARTED | NOT STUDIED / PENDING |
| **Module 4** | Databricks Auto Loader Architecture + Jobs + CI + Final QA | NOT STARTED | NOT STARTED | NOT STUDIED / PENDING |

---

## Documentation Map

1. [01_CDC_FOUNDATIONS.md](./01_CDC_FOUNDATIONS.md)
   - Change Data Capture principles, snapshots vs change feeds, event envelopes, operations (`INSERT`, `UPDATE`, `DELETE`), deterministic event hashing, and sequence ordering.
2. [02_INCREMENTAL_PROCESSING.md](./02_INCREMENTAL_PROCESSING.md)
   - Incremental batching, checkpoints vs high-water marks vs streaming event-time watermarks, idempotency, atomic state writes, and failure safety.
3. [03_EVENT_ORDERING_DEDUP.md](./03_EVENT_ORDERING_DEDUP.md)
   - Duplicate events vs legitimate sequential updates to the same key, physical arrival order vs source sequence, deterministic `ROW_NUMBER()` deduplication, and late-arriving event handling.
4. [IMPLEMENTATION_MAP.md](./IMPLEMENTATION_MAP.md)
   - Complete trace from design requirement to source tables, code implementation files, output artifacts, and architectural rationale.
5. [INTERVIEW_QA.md](./INTERVIEW_QA.md)
   - Deep-dive interview questions and concise, production-ready answers covering CDC, LSNs, deduplication, checkpoints, and watermarks.
