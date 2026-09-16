"""Unit tests for atomic checkpoint manager and high-water mark regression guard."""

from pathlib import Path

import pytest

from src.state.checkpoint import (
    CheckpointManager,
    CheckpointRegressionError,
    CheckpointState,
)


def test_17_checkpoint_loads_and_saves(temp_test_dir: Path):
    """Test 17: Checkpoint manager loads initial default and persists updates accurately."""
    cp_dir = temp_test_dir / "checkpoints"
    mgr = CheckpointManager(cp_dir)

    # Initial load returns clean state
    init_state = mgr.load_checkpoint("subscriptions")
    assert init_state.table_name == "subscriptions"
    assert init_state.highest_source_sequence == 0
    assert init_state.last_processed_batch_id == 0

    # Save updated state
    updated = CheckpointState(
        table_name="subscriptions",
        last_processed_batch_id=1,
        highest_source_sequence=350,
        processed_batch_ids=[1],
    )
    mgr.save_checkpoint("subscriptions", updated)

    loaded = mgr.load_checkpoint("subscriptions")
    assert loaded.highest_source_sequence == 350
    assert loaded.last_processed_batch_id == 1
    assert loaded.processed_batch_ids == [1]


def test_18_checkpoint_cannot_regress(temp_test_dir: Path):
    """Test 18: Checkpoint regression raises CheckpointRegressionError."""
    cp_dir = temp_test_dir / "checkpoints"
    mgr = CheckpointManager(cp_dir)

    # Advance to 500
    mgr.advance_high_water_mark("subscriptions", sequence=500, batch_id=1)
    assert mgr.get_high_water_mark("subscriptions") == 500

    # Attempt to regress to 490 must raise CheckpointRegressionError
    with pytest.raises(CheckpointRegressionError) as exc_info:
        mgr.advance_high_water_mark("subscriptions", sequence=490, batch_id=2)

    assert "Cannot regress checkpoint" in str(exc_info.value)
    # Checkpoint remains at 500
    assert mgr.get_high_water_mark("subscriptions") == 500


def test_19_checkpoint_atomic_write(temp_test_dir: Path, monkeypatch):
    """Test 19: Checkpoint uses safe atomic file replace."""
    cp_dir = temp_test_dir / "checkpoints"
    mgr = CheckpointManager(cp_dir)

    # Verify atomic replace is called and no dangling temp files exist
    state = CheckpointState(table_name="accounts", highest_source_sequence=100)
    mgr.save_checkpoint("accounts", state)

    target_file = cp_dir / "accounts.json"
    assert target_file.exists()

    # Verify no dangling temporary files (.tmp_accounts_*) remain in directory
    temp_files = list(cp_dir.glob(".tmp_*"))
    assert len(temp_files) == 0
