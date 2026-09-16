# 02: Incremental Processing & Checkpoint Design

> **Learning Status**: NOT STUDIED / PENDING

---

## 1. Full Refresh vs Incremental Processing

| Dimension | Full Refresh (Batch Recompute) | Incremental Processing (Delta Ingestion) |
| :--- | :--- | :--- |
| **Data Scanned** | Scans 100% of the historical dataset on every run. | Scans only new/modified rows ($> \text{High-Water Mark}$). |
| **Compute Cost** | Scales linearly ($O(N)$) with total historical data volume. | Scales with delta volume ($O(\Delta N)$), independent of total size. |
| **Latency** | High (hours/overnight batches). | Low (minutes, seconds, or streaming). |
| **Resource Contention** | High I/O and memory pressure on lakehouse clusters. | Predictable, lightweight resource footprint. |
| **Failure Recovery** | Re-running requires recomputing the entire history. | Re-running resumes from the last committed checkpoint. |

---

## 2. Checkpoints vs High-Water Marks vs Streaming Watermarks

A critical architectural distinction frequently tested in data engineering interviews:

### A. Incremental High-Water Mark (HWM)
- **Definition**: The highest monotonic sequence number or source timestamp successfully processed and committed for a given table.
- **Example in Project**: In `state/checkpoints/subscriptions.json`, `highest_source_sequence: 540` indicates all events up to sequence 540 have been validated and ingested.
- **Behavior**: New batches query: `WHERE source_sequence > 540`.

### B. Checkpoint
- **Definition**: Persistent state metadata capturing pipeline progress. In our file-based checkpoint manager ([checkpoint.py](../src/state/checkpoint.py)), it stores `last_processed_batch_id`, `highest_source_sequence`, and the set of `processed_batch_ids`.

### C. Spark Structured Streaming Event-Time Watermark
- **Definition**: A moving temporal threshold calculated dynamically by Spark engine: `max(eventTime) - delayThreshold`.
- **Purpose**: Defines when the streaming engine assumes all late data for a given window has arrived, enabling Spark to evict old state from stateful operators (e.g. streaming aggregations and stream-stream joins) to prevent unbounded memory growth.
- **Difference**: Our high-water mark tracks processed transaction log positions for batch boundary discovery; Spark's event-time watermark is an in-memory state-eviction mechanism for unbounded streaming windows.

---

## 3. Idempotency & Batch Replay

### Definition of Idempotency
An operation is idempotent if applying it multiple times produces the exact same state as applying it once:
$$f(f(x)) = f(x)$$

### How Idempotency is Guaranteed in this Architecture:
1. **Batch ID Tracking**: Every batch contains a deterministic integer ID (`batch_id`). When a batch successfully finishes, its ID is appended to `processed_batch_ids` in the persistent checkpoint.
2. **Re-run Guard**: If a pipeline execution is re-triggered on unchanged storage, `is_batch_completed_for_all(batch_id)` detects the completed state and skips the batch entirely ($0$ new records processed).
3. **Deterministic Event IDs**: If the same batch files are re-processed via replay flags, the deterministic hash `event_id` ensures the deduplication engine retains exactly one copy.

---

## 4. Checkpoint Atomicity & Failure Safety

### The Failure Risk:
If a pipeline updates a checkpoint *before* writing the data, a crash causes data loss (unwritten data skipped on retry). If it writes data and updates the checkpoint via an unbuffered direct overwrite, a mid-write crash can leave corrupted, unparseable JSON on disk.

### The Solution:
1. **Ordered Progression**: Checkpoint advances **ONLY** after validation, deduplication, change ordering, output writing, and metrics reconciliation completely succeed.
2. **Atomic Write Protocol (POSIX Rename)**:
   ```python
   # 1. Write to temporary file in same directory
   temp_fd, temp_path = tempfile.mkstemp(prefix=f".tmp_{table}_", dir=str(checkpoint_dir))
   with open(temp_fd, "w") as f:
       f.write(payload)
       f.flush()
       os.fsync(f.fileno())  # Flush OS buffers to physical disk

   # 2. Atomic filesystem rename
   os.replace(temp_path, target_path)
   ```
3. **Regression Guard**:
   If an incoming sequence is lower than the current checkpoint sequence, the manager raises `CheckpointRegressionError` to prevent backwards time travel.
