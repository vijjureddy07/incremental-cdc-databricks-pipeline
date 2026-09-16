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
class CheckpointState:
    """Incremental processing checkpoint state for a single source table."""

    table_name: str
    last_processed_batch_id: int = 0
    highest_source_sequence: int = 0
    processed_batch_ids: List[int] = field(default_factory=list)
    last_updated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CheckpointState":
        return cls(
            table_name=data["table_name"],
            last_processed_batch_id=data.get("last_processed_batch_id", 0),
            highest_source_sequence=data.get("highest_source_sequence", 0),
            processed_batch_ids=data.get("processed_batch_ids", []),
            last_updated_at=data.get("last_updated_at", datetime.now(timezone.utc).isoformat()),
        )


class CheckpointManager:
    """Manages persistent table checkpoints with atomic writes and regression protection."""

    def __init__(self, checkpoint_dir: Optional[Path] = None):
        self.checkpoint_dir = checkpoint_dir or DEFAULT_CHECKPOINT_DIR
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)

    def _get_file_path(self, table: str) -> Path:
        return self.checkpoint_dir / f"{table}.json"

    def load_checkpoint(self, table: str) -> CheckpointState:
        """Load checkpoint state for a table, returning initial state if not found."""
        path = self._get_file_path(table)
        if not path.exists():
            return CheckpointState(table_name=table)

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return CheckpointState.from_dict(data)

    def save_checkpoint(self, table: str, state: CheckpointState) -> None:
        """Atomically persist checkpoint state to disk using a temporary file and fsync."""
        target_path = self._get_file_path(table)
        payload = json.dumps(state.to_dict(), indent=2)

        # Write to temporary file in the same directory for atomic rename
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

            # Atomic replace
            os.replace(temp_path, target_path)
        except Exception:
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise

    def get_high_water_mark(self, table: str) -> int:
        """Retrieve highest source sequence processed for the given table."""
        return self.load_checkpoint(table).highest_source_sequence

    def advance_high_water_mark(
        self,
        table: str,
        sequence: int,
        batch_id: int,
        force_allow_equal: bool = True,
    ) -> CheckpointState:
        """Advance high water mark safely.

        Raises CheckpointRegressionError if sequence is less than current high water mark.
        """
        current_state = self.load_checkpoint(table)
        current_hwm = current_state.highest_source_sequence

        if sequence < current_hwm:
            raise CheckpointRegressionError(
                f"Cannot regress checkpoint for table '{table}': "
                f"new sequence ({sequence}) < current high-water mark ({current_hwm})"
            )

        if sequence == current_hwm and not force_allow_equal:
            raise CheckpointRegressionError(
                f"Sequence ({sequence}) equals current high-water mark ({current_hwm}) for table '{table}'"
            )

        new_batches = list(current_state.processed_batch_ids)
        if batch_id not in new_batches:
            new_batches.append(batch_id)
            new_batches.sort()

        updated_state = CheckpointState(
            table_name=table,
            last_processed_batch_id=batch_id,
            highest_source_sequence=max(current_hwm, sequence),
            processed_batch_ids=new_batches,
            last_updated_at=datetime.now(timezone.utc).isoformat(),
        )
        self.save_checkpoint(table, updated_state)
        return updated_state

    def is_batch_completed_for_all(self, batch_id: int, tables: Optional[List[str]] = None) -> bool:
        """Check if a batch_id is recorded in all table checkpoints."""
        check_tables = tables or list(SUPPORTED_TABLES)
        for t in check_tables:
            cp = self.load_checkpoint(t)
            if batch_id not in cp.processed_batch_ids:
                return False
        return True
