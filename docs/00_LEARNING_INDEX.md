# Learning Index: Incremental CDC Databricks Pipeline

> [!IMPORTANT]
> **Workflow Policy**: BUILD FIRST → DOCUMENT EVERYTHING → LEARN LATER.
> All learning statuses remain **NOT STUDIED / PENDING** until explicitly reviewed with the user. No quizzing.

---

## Module Progress & Learning Status

| Module | Focus Area | Build Status | Test Status | Learning Status |
| :--- | :--- | :--- | :--- | :--- |
| **Module 1** | **CDC Source Simulation + Incremental Change Feed Foundation** | **BUILD COMPLETE** | **24 pytest items passing** | **NOT STUDIED / PENDING** |
| **Module 2** | **Delta MERGE + Current-State Tables + Delete Handling** | **BUILD COMPLETE** | **18 pytest items passing** (40 Module 2 verification requirements covered) | **NOT STUDIED / PENDING** |
| **Module 3** | **Late Events + SCD2 + Schema Evolution + Replay / Recovery** | **BUILD COMPLETE**<br>*(LOCAL VERIFIED)* | **15 pytest items passing** (All Module 3 verification requirements covered) | **NOT STUDIED / PENDING** |
| **Module 4** | Databricks Auto Loader Architecture + Jobs + CI + Final QA | NOT STARTED | NOT STARTED | NOT STUDIED / PENDING |

> **Global Regression Metric**: Exactly **57 pytest items collected and passing** (`57 passed, 0 failed` across entire test suite).

---

## Documentation Map

1. [01_CDC_FOUNDATIONS.md](./01_CDC_FOUNDATIONS.md)
   - Change Data Capture principles, snapshots vs change feeds, event envelopes, operations (`INSERT`, `UPDATE`, `DELETE`), deterministic event hashing, and sequence ordering.
2. [02_INCREMENTAL_PROCESSING.md](./02_INCREMENTAL_PROCESSING.md)
   - Incremental batching, checkpoints vs high-water marks vs streaming event-time watermarks, idempotency, atomic state writes, and failure safety.
3. [03_EVENT_ORDERING_DEDUP.md](./03_EVENT_ORDERING_DEDUP.md)
   - Duplicate events vs legitimate sequential updates to the same key, physical arrival order vs source sequence, deterministic `ROW_NUMBER()` deduplication, and late-arriving event handling.
4. [04_DELTA_CURRENT_STATE.md](./04_DELTA_CURRENT_STATE.md)
   - Delta Lake storage layer, transaction log architecture, Parquet vs Delta, current-state tables, Delta MERGE, soft-delete tombstones, protective tombstones, full after-images, and Decimal monetary representations.
5. [05_CDC_MERGE_IDEMPOTENCY.md](./05_CDC_MERGE_IDEMPOTENCY.md)
   - Idempotent CDC processing, within-batch collapsing (`SUPERSEDED_WITHIN_BATCH`), target/ledger crash gap recovery (`RECOVERED_AFTER_PARTIAL_COMMIT`), re-insert after delete, applied event ledger, and atomic global batch checkpoints.
6. [06_LATE_EVENT_REPLAY_SCD2.md](./06_LATE_EVENT_REPLAY_SCD2.md)
   - Late-arriving CDC events, arrival vs source order, canonical event store, late-event replay queue, operational SCD2 history, source-sequence intervals, key-scoped timeline rebuild, sequence collision handling, replay idempotency, and crash recovery.
7. [07_SCHEMA_EVOLUTION_RECOVERY.md](./07_SCHEMA_EVOLUTION_RECOVERY.md)
   - Versioned payload schemas (V1/V2), controlled Delta schema migration, why global autoMerge is risky, unknown schema quarantine, full after-image scope across versions, replay across schemas, and checkpoint isolation.
8. [PROGRESS.md](./PROGRESS.md)
   - Factual progress tracking across Build, Test, and Learning statuses for all pipeline modules.
9. [IMPLEMENTATION_MAP.md](./IMPLEMENTATION_MAP.md)
   - Complete trace from design requirement to source tables, code implementation files, output artifacts, and architectural rationale.
10. [INTERVIEW_QA.md](./INTERVIEW_QA.md)
   - 41 deep-dive production data engineering interview questions & answers covering CDC, LSNs, deduplication, checkpoints, watermarks, Delta MERGE, crash recovery, idempotency, SCD2 history, late replay, and schema evolution.

