# Implementation Map: Concept to Code Traceability

> **Learning Status**: NOT STUDIED / PENDING

This document provides complete, line-by-line architectural traceability linking every conceptual requirement to its source implementation, pipeline stage, and output artifact.

---

## Traceability Matrix

| # | Concept / Requirement | Source Table / Batch | File & Function | Output Artifact | Architectural Why |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **1** | **Deterministic Initial State** | `accounts`, `subscriptions`, `invoices`, `payments` | [initial_state.py](../src/generation/initial_state.py)<br>`generate_initial_state()` | `data/sample/snapshot/*.jsonl`<br>`snapshot_metadata.json` | Guarantees test reproducibility: fixed seed + scale yields identical records and sequence counters. |
| **2** | **CDC Batch Generation & Envelope** | All 4 domain entities | [cdc_generator.py](../src/generation/cdc_generator.py)<br>`generate_batch()` | `data/generated/cdc/batch_id=XXXXXX/*.jsonl`<br>`manifest.json` | Simulates realistic OLTP mutations (inserts, updates, deletes) with manifest metadata for ingestion validation. |
| **3** | **Deterministic Event ID** | All change events | [cdc_schema.py](../src/schemas/cdc_schema.py)<br>`generate_event_id()` | `CDCEvent.event_id` | SHA-256 hash of immutable change coordinates enables exact deduplication across retries. |
| **4** | **Source Sequence (Simulated LSN)** | All change events | [cdc_generator.py](../src/generation/cdc_generator.py)<br>`_next_seq()` | `CDCEvent.source_sequence` | Simulates WAL/binlog position, providing absolute ground truth for mutation ordering. |
| **5** | **Explicit PySpark Schema** | Change event ingestion | [cdc_schema.py](../src/schemas/cdc_schema.py)<br>`CDC_SPARK_SCHEMA` | DataFrame with fixed `StructType` | Prevents runtime schema inference overhead, type coercion bugs, and corrupted data ingestion. |
| **6** | **Batch Discovery & Ordering** | CDC batch directories | [discovery.py](../src/cdc/discovery.py)<br>`discover_batches()` | `List[DiscoveredBatch]` sorted numerically | Ensures batch 9 is processed before batch 10 and rejects malformed directories. |
| **7** | **Schema Validation & Quarantine** | All ingested events | [validation.py](../src/cdc/validation.py)<br>`validate_cdc_dataframe()` | `output/valid_events/`<br>`output/quarantine/` | Invalid/corrupt records are safely quarantined with exact rejection reasons rather than silently dropped. |
| **8** | **Deterministic Deduplication** | Events with duplicate IDs | [deduplication.py](../src/cdc/deduplication.py)<br>`deduplicate_cdc_events()` | `deduped_valid_df`<br>`duplicates_df` | `ROW_NUMBER()` over `event_id` ordered by ingestion timestamp provides deterministic selection across cluster workers. |
| **9** | **Source-Sequence Ordering** | Valid change stream | [ordering.py](../src/cdc/ordering.py)<br>`order_cdc_events()` | Ordered DataFrame by `(table, key, seq)` | Solves out-of-order physical arrival by restoring true logical state progression. |
| **10** | **High-Water Mark & Atomicity** | Incremental progress | [checkpoint.py](../src/state/checkpoint.py)<br>`CheckpointManager` | `state/checkpoints/{table}.json` | Atomic rename (`os.replace`) prevents corrupt state files; advances only upon 100% pipeline success. |
| **11** | **Regression Protection** | Sequence verification | [checkpoint.py](../src/state/checkpoint.py)<br>`advance_high_water_mark()` | `CheckpointRegressionError` | Prevents backwards time travel or accidental state rewind. |
| **12** | **Late-Event Isolation** | Events with `seq <= HWM` | [processor.py](../src/cdc/processor.py)<br>`process_batch()` | `output/late_events/` | Late-arriving events are preserved for Module 3 reconciliation rather than dropped. |
| **13** | **Metrics Reconciliation** | Batch summary | [processor.py](../src/cdc/processor.py)<br>`process_batch()` | `output/metrics/batch_id=XXXXXX/metrics.json` | Verifies invariant: `raw == valid_unique + duplicates + invalid`. |
| **14** | **Diagnostic Analytics** | Processed outputs | [analysis.py](../src/cdc/analysis.py)<br>`CDCAnalyzer` | Temporary views (`v_cdc_valid`, `v_cdc_quarantine`, `v_cdc_late`) | Diagnostic aggregations for operations, sequences, update frequencies, and quarantine reasons. |
