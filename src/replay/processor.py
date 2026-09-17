"""Replay Processor executing deterministic key-scoped historical reconstruction with crash recovery."""

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from pyspark.sql import SparkSession

from src.config.settings import (
    DEFAULT_CDC_EVENT_STORE_DIR,
    DEFAULT_CURRENT_DIR,
    DEFAULT_SAMPLE_DATA_DIR,
    DEFAULT_SUBSCRIPTIONS_HISTORY_DIR,
)
from src.history.subscriptions_scd2 import (
    CurrentStateDriftError,
    HistoryInvariantError,
    SourceSequenceConflictError,
)
from src.replay.audit import generate_replay_id, record_replay_audit
from src.replay.history_rebuilder import reconstruct_subscription_timeline_for_key
from src.replay.queue import (
    STATUS_APPLIED,
    STATUS_CONFLICT,
    STATUS_FAILED,
    load_pending_replay_events,
    update_queue_status,
)

logger = logging.getLogger(__name__)


def process_pending_replays(
    spark: SparkSession,
    replay_dir: Optional[Path] = None,
    event_store_dir: Optional[Path] = None,
    history_dir: Optional[Path] = None,
    current_delta_dir: Optional[Path] = None,
    snapshot_dir: Optional[Path] = None,
    repair_current: bool = False,
) -> Dict[str, Any]:
    """Process all pending items in the late event replay queue.

    CRITICAL ISOLATION RULE:
    Replay is a historical correction lane. It MUST NEVER call commit_batch() or mutate
    the global incremental checkpoint state.

    CRASH RECOVERY / IDEMPOTENCY:
    If history reconstruction previously succeeded but the process crashed before queue status
    or replay audit was committed, re-running detects existing deterministic history versions,
    re-verifies invariants, and updates queue/audit state without duplicating rows.

    Returns:
        Summary dict containing counts and replay details.
    """
    pending_df = load_pending_replay_events(spark, replay_dir)
    pending_events = [r.asDict() for r in pending_df.collect()]

    summary = {
        "total_pending": len(pending_events),
        "applied": 0,
        "conflict": 0,
        "failed": 0,
        "processed_event_ids": [],
    }

    for ev in pending_events:
        event_id = ev["event_id"]
        source_table = ev["source_table"]
        business_key = ev["business_key"]
        source_sequence = ev["source_sequence"]
        attempt = ev.get("replay_attempt_count", 0) + 1

        started_at = datetime.now(timezone.utc).isoformat()
        replay_id = generate_replay_id(event_id)

        if source_table != "subscriptions":
            logger.info("Replay for table '%s' is not supported; skipping.", source_table)
            continue

        try:
            timeline, metrics = reconstruct_subscription_timeline_for_key(
                spark=spark,
                subscription_id=business_key,
                snapshot_dir=snapshot_dir or DEFAULT_SAMPLE_DATA_DIR,
                event_store_dir=event_store_dir or DEFAULT_CDC_EVENT_STORE_DIR,
                history_dir=history_dir or DEFAULT_SUBSCRIPTIONS_HISTORY_DIR,
                current_delta_dir=current_delta_dir or DEFAULT_CURRENT_DIR,
                repair_current=repair_current,
            )

            completed_at = datetime.now(timezone.utc).isoformat()
            audit_record = {
                "replay_id": replay_id,
                "event_id": event_id,
                "source_table": source_table,
                "business_key": business_key,
                "source_sequence": source_sequence,
                "status": STATUS_APPLIED,
                "history_versions_before": metrics["history_versions_before"],
                "history_versions_after": metrics["history_versions_after"],
                "current_sequence_before": metrics["current_sequence_before"],
                "current_sequence_after": metrics["current_sequence_after"],
                "current_state_changed": metrics["current_state_changed"],
                "attempt_number": attempt,
                "started_at": started_at,
                "completed_at": completed_at,
                "failure_reason": None,
            }
            record_replay_audit(spark, audit_record, replay_dir)
            update_queue_status(
                spark, event_id, STATUS_APPLIED, failure_reason=None, replay_dir=replay_dir
            )

            summary["applied"] += 1
            summary["processed_event_ids"].append(event_id)
            logger.info("Successfully replayed late event %s for key %s", event_id, business_key)

        except SourceSequenceConflictError as conflict_exc:
            completed_at = datetime.now(timezone.utc).isoformat()
            err_msg = str(conflict_exc)
            audit_record = {
                "replay_id": replay_id,
                "event_id": event_id,
                "source_table": source_table,
                "business_key": business_key,
                "source_sequence": source_sequence,
                "status": STATUS_CONFLICT,
                "history_versions_before": 0,
                "history_versions_after": 0,
                "current_sequence_before": 0,
                "current_sequence_after": 0,
                "current_state_changed": False,
                "attempt_number": attempt,
                "started_at": started_at,
                "completed_at": completed_at,
                "failure_reason": err_msg,
            }
            record_replay_audit(spark, audit_record, replay_dir)
            update_queue_status(
                spark, event_id, STATUS_CONFLICT, failure_reason=err_msg, replay_dir=replay_dir
            )

            summary["conflict"] += 1
            logger.warning("Conflict during replay of event %s: %s", event_id, err_msg)

        except (HistoryInvariantError, CurrentStateDriftError, Exception) as exc:
            completed_at = datetime.now(timezone.utc).isoformat()
            err_msg = str(exc)
            audit_record = {
                "replay_id": replay_id,
                "event_id": event_id,
                "source_table": source_table,
                "business_key": business_key,
                "source_sequence": source_sequence,
                "status": STATUS_FAILED,
                "history_versions_before": 0,
                "history_versions_after": 0,
                "current_sequence_before": 0,
                "current_sequence_after": 0,
                "current_state_changed": False,
                "attempt_number": attempt,
                "started_at": started_at,
                "completed_at": completed_at,
                "failure_reason": err_msg,
            }
            record_replay_audit(spark, audit_record, replay_dir)
            update_queue_status(
                spark, event_id, STATUS_FAILED, failure_reason=err_msg, replay_dir=replay_dir
            )

            summary["failed"] += 1
            logger.error("Failed replaying late event %s: %s", event_id, err_msg)

    return summary
