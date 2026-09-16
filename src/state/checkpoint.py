"""Atomic and regression-safe incremental processing checkpoint manager."""

import json
import os
import tempfile
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.config.settings import DEFAULT_CHECKPOINT_DIR, SUPPORTED_TABLES


class CheckpointRegressionError(ValueError):
    """Raised when an attempt is made to regress the high-water mark sequence."""

    pass


@dataclass
class TableState:
    """State for an individual table within the global checkpoint."""

    highest_source_sequence: int = 0
    last_processed_batch_id: int = 0
    last_updated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TableState":
        return cls(
            highest_source_sequence=data.get("highest_source_sequence", 0),
            last_processed_batch_id=data.get("last_processed_batch_id", 0),
            last_updated_at=data.get("last_updated_at", datetime.now(timezone.utc).isoformat()),
        )


@dataclass
class GlobalCheckpointState:
    """Unified checkpoint state across all source tables committed atomically."""

    version: int = 1
    tables: Dict[str, TableState] = field(default_factory=dict)
    completed_batch_ids: List[int] = field(default_factory=list)
    last_updated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "tables": {t: s.to_dict() for t, s in self.tables.items()},
            "completed_batch_ids": sorted(self.completed_batch_ids),
            "last_updated_at": self.last_updated_at,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "GlobalCheckpointState":
        tables_dict = {}
        for t_name, t_data in data.get("tables", {}).items():
            tables_dict[t_name] = TableState.from_dict(t_data)

        return cls(
            version=data.get("version", 1),
            tables=tables_dict,
            completed_batch_ids=data.get("completed_batch_ids", []),
            last_updated_at=data.get("last_updated_at", datetime.now(timezone.utc).isoformat()),
        )


@dataclass
class CheckpointState:
    """Incremental processing checkpoint state for a single source table (compatibility view)."""

    table_name: str
    last_processed_batch_id: int = 0
    highest_source_sequence: int = 0
    processed_batch_ids: List[int] = field(default_factory=list)
    last_updated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class CheckpointManager:
    """Manages persistent pipeline checkpoints with single-file batch atomicity and regression protection.

    ARCHITECTURAL ADVANCEMENT (Module 1 Hardening / Module 2):
    Instead of writing separate JSON files per table (which could leave a partially committed batch
    if a failure occurs mid-write), this manager maintains a single atomic checkpoint state file:
    `checkpoint_state.json`.

    When `commit_batch` is executed:
    1. Loads existing state.
    2. Validates all proposed sequence updates against current high-water marks.
    3. Rejects any regression BEFORE touching storage (fails closed).
    4. Builds complete new state representation in memory.
    5. Writes to a temporary file in the checkpoint directory.
    6. Flushes and executes `os.fsync` to force OS write buffers to disk.
    7. Atomically replaces `checkpoint_state.json` via POSIX `os.replace`.

    Result: Either ALL table checkpoints and batch completion state advance, or NONE advance.
    """

    STATE_FILENAME = "checkpoint_state.json"

    def __init__(self, checkpoint_dir: Optional[Path] = None):
        self.checkpoint_dir = checkpoint_dir or DEFAULT_CHECKPOINT_DIR
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.state_file_path = self.checkpoint_dir / self.STATE_FILENAME

    def _migrate_legacy_files_if_needed(self) -> Optional[GlobalCheckpointState]:
        """Auto-migrate legacy per-table JSON files if state_file_path does not exist."""
        if self.state_file_path.exists():
            return None

        migrated_tables: Dict[str, TableState] = {}
        all_completed_batches: set = set()

        for table in SUPPORTED_TABLES:
            legacy_path = self.checkpoint_dir / f"{table}.json"
            if legacy_path.exists():
                try:
                    with open(legacy_path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    migrated_tables[table] = TableState(
                        highest_source_sequence=data.get("highest_source_sequence", 0),
                        last_processed_batch_id=data.get("last_processed_batch_id", 0),
                        last_updated_at=data.get(
                            "last_updated_at", datetime.now(timezone.utc).isoformat()
                        ),
                    )
                    all_completed_batches.update(data.get("processed_batch_ids", []))
                except Exception:
                    pass

        if migrated_tables:
            initial_state = GlobalCheckpointState(
                version=1,
                tables=migrated_tables,
                completed_batch_ids=sorted(all_completed_batches),
                last_updated_at=datetime.now(timezone.utc).isoformat(),
            )
            self._save_global_state(initial_state)
            return initial_state
        return None

    def load_global_state(self) -> GlobalCheckpointState:
        """Load the unified global checkpoint state, migrating legacy state if present."""
        migrated = self._migrate_legacy_files_if_needed()
        if migrated is not None:
            return migrated

        if not self.state_file_path.exists():
            # Initialize with default tables
            init_tables = {
                t: TableState(highest_source_sequence=0, last_processed_batch_id=0)
                for t in SUPPORTED_TABLES
            }
            return GlobalCheckpointState(
                version=1,
                tables=init_tables,
                completed_batch_ids=[],
                last_updated_at=datetime.now(timezone.utc).isoformat(),
            )

        with open(self.state_file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return GlobalCheckpointState.from_dict(data)

    def _save_global_state(self, state: GlobalCheckpointState) -> None:
        """Atomically persist global checkpoint state using a temporary file and fsync."""
        payload = json.dumps(state.to_dict(), indent=2)

        temp_fd, temp_path = tempfile.mkstemp(
            prefix=".tmp_checkpoint_state_",
            suffix=".json",
            dir=str(self.checkpoint_dir),
        )
        try:
            with open(temp_fd, "w", encoding="utf-8") as f:
                f.write(payload)
                f.flush()
                os.fsync(f.fileno())

            # Atomic rename / replace
            os.replace(temp_path, self.state_file_path)
        except Exception:
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except OSError:
                    pass
            raise

    def load_checkpoint(self, table: str) -> CheckpointState:
        """Compatibility method: load checkpoint state for a single table."""
        global_state = self.load_global_state()
        t_state = global_state.tables.get(table, TableState())
        return CheckpointState(
            table_name=table,
            last_processed_batch_id=t_state.last_processed_batch_id,
            highest_source_sequence=t_state.highest_source_sequence,
            processed_batch_ids=list(global_state.completed_batch_ids),
            last_updated_at=t_state.last_updated_at,
        )

    def save_checkpoint(self, table: str, state: CheckpointState) -> None:
        """Compatibility method: atomically save single-table state into global state and table file."""
        self.commit_batch(
            batch_id=state.last_processed_batch_id,
            table_sequences={table: state.highest_source_sequence},
        )
        target_path = self.checkpoint_dir / f"{table}.json"
        payload = json.dumps(state.to_dict(), indent=2)
        temp_fd, temp_path = tempfile.mkstemp(
            prefix=f".tmp_{table}_",
            suffix=".json",
            dir=str(self.checkpoint_dir),
        )
        try:
            with open(temp_fd, "w", encoding="utf-8") as f:
                f.write(payload)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, target_path)
        except Exception:
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except OSError:
                    pass
            raise

    def get_high_water_mark(self, table: str) -> int:
        """Retrieve highest source sequence processed for the given table."""
        global_state = self.load_global_state()
        return global_state.tables.get(table, TableState()).highest_source_sequence

    def get_completed_batches(self) -> List[int]:
        """Retrieve list of batch IDs that have been fully processed and committed."""
        return list(self.load_global_state().completed_batch_ids)

    def is_batch_completed(self, batch_id: int) -> bool:
        """Check if a specific batch ID has already completed."""
        return batch_id in self.load_global_state().completed_batch_ids

    def is_batch_completed_for_all(self, batch_id: int, tables: Optional[List[str]] = None) -> bool:
        """Check if batch_id is completed in the global checkpoint."""
        return self.is_batch_completed(batch_id)

    def advance_high_water_mark(
        self,
        table: str,
        sequence: int,
        batch_id: int,
        force_allow_equal: bool = True,
    ) -> CheckpointState:
        """Advance single table high-water mark via atomic batch commit."""
        self.commit_batch(
            batch_id=batch_id,
            table_sequences={table: sequence},
        )
        return self.load_checkpoint(table)

    def commit_batch(
        self,
        batch_id: int,
        table_sequences: Dict[str, int],
    ) -> GlobalCheckpointState:
        """Atomically advance all table high-water marks and record completed batch.

        CRITICAL ATOMICITY GUARANTEE:
        1. Validates EVERY proposed sequence against current table high-water marks.
        2. Raises CheckpointRegressionError before touching disk if any sequence regresses.
        3. Prepares the complete unified state in memory.
        4. Writes atomically via tempfile + fsync + os.replace.

        Either ALL table checkpoints advance, or NONE advance.
        """
        current_state = self.load_global_state()
        now_iso = datetime.now(timezone.utc).isoformat()

        # Phase 1: Validate no regression across ANY requested table update
        for table, new_seq in table_sequences.items():
            current_hwm = current_state.tables.get(table, TableState()).highest_source_sequence
            if new_seq < current_hwm:
                raise CheckpointRegressionError(
                    f"Cannot regress checkpoint for table '{table}': "
                    f"new sequence ({new_seq}) < current high-water mark ({current_hwm})"
                )

        # Phase 2: Assemble complete new state dictionary
        new_tables = dict(current_state.tables)
        for table, new_seq in table_sequences.items():
            prev_table_state = new_tables.get(table, TableState())
            new_tables[table] = TableState(
                highest_source_sequence=max(prev_table_state.highest_source_sequence, new_seq),
                last_processed_batch_id=batch_id,
                last_updated_at=now_iso,
            )

        new_completed = list(current_state.completed_batch_ids)
        if batch_id not in new_completed:
            new_completed.append(batch_id)
            new_completed.sort()

        new_state = GlobalCheckpointState(
            version=1,
            tables=new_tables,
            completed_batch_ids=new_completed,
            last_updated_at=now_iso,
        )

        # Phase 3: Atomic write to single file
        self._save_global_state(new_state)
        return new_state
