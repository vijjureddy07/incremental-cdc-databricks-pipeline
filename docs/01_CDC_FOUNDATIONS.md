# 01: CDC Foundations & Architecture

> **Learning Status**: NOT STUDIED / PENDING

---

## 1. What is Change Data Capture (CDC)?

### Concept Overview
Change Data Capture (CDC) is a design pattern and set of software technologies that identify and capture state changes (inserts, updates, and deletes) made to an operational database (OLTP) and deliver those changes in real time or near-real time to downstream consumers (data lakes, event buses, search indexes, or analytical warehouses).

### Snapshot vs Change Event Feed
- **Initial Snapshot (State-at-Rest)**: Represents the full state of every entity at a specific point in time ($T_0$). It contains the cumulative current state of all active records.
- **Change Feed (State-in-Motion)**: Represents discrete, ordered state transitions ($T_1, T_2, T_3, \dots$). Each event documents a change delta with an operation (`INSERT`, `UPDATE`, `DELETE`), sequence number, and timestamp.

---

## 2. Deep Dive: Major Architectural Concepts

### Concept A: Source Sequence (Simulated LSN)
- **WHAT IT IS**: A strictly monotonic, sequential integer assigned by the source transaction log to every discrete database mutation.
- **WHY IT EXISTS**: Distributed messaging systems and file storage do not guarantee delivery order. `source_sequence` provides the absolute, deterministic ground truth for change ordering.
- **DISTINCTION FROM REAL POSTGRESQL LSN**: A real PostgreSQL Log Sequence Number (LSN) is a 64-bit integer representing a byte offset within the Write-Ahead Log (WAL) file (e.g., `16/B374D848`). In this project, `source_sequence` simulates this ordering behavior deterministically without requiring a live PostgreSQL instance.
- **WHERE USED**: [cdc_generator.py](../src/generation/cdc_generator.py), [ordering.py](../src/cdc/ordering.py), [checkpoint.py](../src/state/checkpoint.py).
- **EXACT FILE / FUNCTION**: `src/cdc/ordering.py::order_cdc_events()`
- **EXAMPLE**: Sequence 101 represents an INSERT; Sequence 102 represents a plan upgrade. Even if Sequence 102 physically arrives first, the consumer orders by sequence to ensure correct state transitions.
- **INTERVIEW QUESTION**: *"Why can't you rely on arrival timestamp or file creation time to order database changes?"*
- **EXPECTED ANSWER**: *"Network jitter, parallel writer threads, and distributed storage ingestion cause physical arrival times to diverge from logical transaction times. Relying on arrival timestamps leads to race conditions where an older update overwrites a newer update. The source database transaction sequence (such as a WAL LSN or monotonic sequence) is the only immutable ordering authority."*

---

### Concept B: Deterministic Event ID
- **WHAT IT IS**: A SHA-256 hash derived exclusively from immutable change coordinates: `SHA256(source_table + business_key + source_sequence + operation)`.
- **WHY IT EXISTS**: Provides idempotency. If a producer re-emits the exact same logical change due to network timeout or retry, the re-emitted record produces an identical `event_id`, enabling exact deduplication.
- **WHERE USED**: [cdc_schema.py](../src/schemas/cdc_schema.py), [deduplication.py](../src/cdc/deduplication.py).
- **EXACT FILE / FUNCTION**: `src/schemas/cdc_schema.py::generate_event_id()`
- **EXAMPLE**:
  ```python
  generate_event_id("subscriptions", "SUB-000100", 540, "UPDATE")
  # Returns: "f4a8b79c3d...64 hex chars"
  ```
- **INTERVIEW QUESTION**: *"What is the difference between a business key and an event ID?"*
- **EXPECTED ANSWER**: *"A business key identifies the real-world business entity (e.g. `account_id = ACC-001`) and remains constant across all changes to that entity. An event ID identifies a single discrete mutation event in time for that entity. An account will have one business key but many event IDs over its lifecycle."*

---

### Concept C: Operation Contract (`INSERT`, `UPDATE`, `DELETE`)
- **WHAT IT IS**: Standardized operation tags defining the state mutation semantics.
- **WHY IT EXISTS**: Informs the downstream lakehouse merge engine whether to append a new record, update an existing current state, or flag/delete a record.
- **WHERE USED**: [cdc_schema.py](../src/schemas/cdc_schema.py), [validation.py](../src/cdc/validation.py).
- **EXACT FILE / FUNCTION**: `src/cdc/validation.py::validate_event_row()`
- **TOMBSTONE / REDUCED DELETE PAYLOAD**: For `DELETE` operations, the source OLTP record no longer exists. The CDC event envelope carries a reduced payload (tombstone) containing only the primary key/business key and optional deletion metadata, preventing artificial schema validation failures on missing business fields.
- **INTERVIEW QUESTION**: *"How should a CDC pipeline handle DELETE events when source systems only emit the primary key?"*
- **EXPECTED ANSWER**: *"The schema validation engine must treat DELETE operations as tombstones, requiring only the primary key in the payload, while enforcing full field validation for INSERT and UPDATE operations. Downstream, the engine applies soft deletes or hard deletes using Delta Lake `DELETE WHERE` / `WHEN MATCHED THEN DELETE`."*

---

### Concept D: Before vs After Images
- **WHAT IT IS**:
  - **Before-image (`update_preimage`)**: The complete row state immediately prior to the update.
  - **After-image (`update_postimage`)**: The row state immediately after the update was applied.
- **WHY IT EXISTS**: Before-images allow consumers to calculate exact metric deltas (e.g., net revenue changes: `after.amount - before.amount`) and audit historical field shifts without loading past table versions.
- **WHERE USED**: Delta Lake Change Data Feed (CDF), explored in Modules 2 and 3. In Module 1, our payload encapsulates post-images with explicit sequence tags.
- **INTERVIEW QUESTION**: *"When is an after-image insufficient in CDC processing?"*
- **EXPECTED ANSWER**: *"When computing streaming aggregations or financial reconciliations where the consumer must subtract the old value before adding the new value. Without the before-image, the consumer must query the target table to discover what value was overwritten, causing expensive random reads."*
