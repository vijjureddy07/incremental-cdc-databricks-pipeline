"""Unit tests for deterministic source snapshot and CDC event generation."""

from pathlib import Path

from src.generation.cdc_generator import generate_cdc_batches
from src.generation.initial_state import generate_initial_state
from src.schemas.cdc_schema import (
    OPERATION_INSERT,
    OPERATION_UPDATE,
    generate_event_id,
)


def test_01_deterministic_initial_state(temp_test_dir: Path):
    """Test 1: Same seed and scale produce identical initial OLTP state."""
    state1 = generate_initial_state(scale="tiny", seed=42)
    state2 = generate_initial_state(scale="tiny", seed=42)

    assert len(state1.accounts) == len(state2.accounts)
    assert len(state1.subscriptions) == len(state2.subscriptions)
    assert len(state1.invoices) == len(state2.invoices)
    assert len(state1.payments) == len(state2.payments)

    assert state1.accounts == state2.accounts
    assert state1.subscriptions == state2.subscriptions
    assert state1.invoices == state2.invoices
    assert state1.payments == state2.payments
    assert state1.highest_initial_sequence == state2.highest_initial_sequence


def test_02_deterministic_cdc_generation(temp_test_dir: Path):
    """Test 2: Same seed and scale produce identical CDC batches and manifests."""
    batches1 = generate_cdc_batches(num_batches=2, scale="tiny", seed=99)
    batches2 = generate_cdc_batches(num_batches=2, scale="tiny", seed=99)

    assert len(batches1) == len(batches2) == 2
    for (data1, man1), (data2, man2) in zip(batches1, batches2):
        assert man1.batch_id == man2.batch_id
        assert man1.event_count == man2.event_count
        assert man1.minimum_source_sequence == man2.minimum_source_sequence
        assert man1.maximum_source_sequence == man2.maximum_source_sequence
        assert data1 == data2


def test_03_same_logical_event_same_event_id():
    """Test 3: Same logical event coordinates deterministically yield identical event_id."""
    id1 = generate_event_id("subscriptions", "SUB-000123", 105, OPERATION_UPDATE)
    id2 = generate_event_id("subscriptions", "SUB-000123", 105, OPERATION_UPDATE)
    assert id1 == id2
    assert len(id1) == 64  # Valid SHA-256 length


def test_04_different_sequence_different_event_id():
    """Test 4: Different source sequence produces a distinct event_id."""
    id_seq1 = generate_event_id("subscriptions", "SUB-000123", 105, OPERATION_UPDATE)
    id_seq2 = generate_event_id("subscriptions", "SUB-000123", 106, OPERATION_UPDATE)
    assert id_seq1 != id_seq2

    id_op_insert = generate_event_id("subscriptions", "SUB-000123", 105, OPERATION_INSERT)
    assert id_seq1 != id_op_insert
