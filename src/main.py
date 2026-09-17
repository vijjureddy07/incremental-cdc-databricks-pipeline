"""Command-line interface for Incremental CDC Databricks Pipeline (Module 1)."""

import argparse
import json
import sys

from src.config.settings import (
    DEFAULT_CDC_DATA_DIR,
    DEFAULT_CHECKPOINT_DIR,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_SAMPLE_DATA_DIR,
    DefectConfig,
)
from src.generation.cdc_generator import generate_cdc_batches
from src.generation.initial_state import generate_initial_state


def cmd_generate_snapshot(args: argparse.Namespace) -> None:
    print(f"Generating initial OLTP snapshot [scale={args.scale}, seed={args.seed}]...")
    state = generate_initial_state(
        scale=args.scale,
        seed=args.seed,
        output_dir=DEFAULT_SAMPLE_DATA_DIR / "snapshot",
    )
    print("✓ Initial snapshot generated successfully:")
    print(f"  - Accounts: {len(state.accounts)}")
    print(f"  - Subscriptions: {len(state.subscriptions)}")
    print(f"  - Invoices: {len(state.invoices)}")
    print(f"  - Payments: {len(state.payments)}")
    print(f"  - Highest Initial Sequence: {state.highest_initial_sequence}")
    print(f"  - Location: {DEFAULT_SAMPLE_DATA_DIR / 'snapshot'}")


def cmd_generate_cdc(args: argparse.Namespace) -> None:
    print(f"Generating {args.batches} CDC batches [scale={args.scale}, seed={args.seed}]...")
    defects = DefectConfig()
    batches = generate_cdc_batches(
        num_batches=args.batches,
        scale=args.scale,
        seed=args.seed,
        output_dir=DEFAULT_CDC_DATA_DIR,
        defects=defects,
    )
    print(f"✓ Generated {len(batches)} CDC batches in {DEFAULT_CDC_DATA_DIR}:")
    for batch_data, manifest in batches:
        print(
            f"  • Batch {manifest.batch_id}: {manifest.event_count} events (seq {manifest.minimum_source_sequence}..{manifest.maximum_source_sequence})"
        )
        if manifest.defects_injected:
            for defect in manifest.defects_injected:
                print(f"    - [DEFECT] {defect}")


def cmd_process_cdc(args: argparse.Namespace) -> None:
    print("Initializing PySpark session and CDC Processor...")
    from src.cdc.processor import CDCProcessor
    from src.utils.spark import get_spark_session, stop_spark_session

    spark = get_spark_session()
    try:
        processor = CDCProcessor(
            spark=spark,
            cdc_dir=DEFAULT_CDC_DATA_DIR,
            checkpoint_dir=DEFAULT_CHECKPOINT_DIR,
            output_dir=DEFAULT_OUTPUT_DIR,
        )
        results = processor.process_all_pending_batches()
        if not results:
            print("✓ No new pending CDC batches found. Pipeline is up to date (idempotent).")
            return

        print(f"✓ Successfully processed {len(results)} new CDC batches:")
        for res in results:
            print(f"  • Batch {res.batch_id}:")
            print(f"    - Raw events: {res.total_raw_events}")
            print(f"    - Valid unique: {res.total_valid_unique}")
            print(f"    - Duplicates dropped: {res.total_duplicates}")
            print(f"    - Quarantined invalid: {res.total_invalid}")
            print(f"    - Late events detected: {res.total_late_events}")
            print("    - Table breakdown:")
            for t_name, tm in res.table_metrics.items():
                print(
                    f"      * {t_name}: raw={tm.raw_event_count}, valid={tm.valid_unique_count}, dup={tm.duplicate_event_count}, inv={tm.invalid_event_count}, late={tm.late_event_count}, seq_range=[{tm.minimum_sequence}..{tm.maximum_sequence}]"
                )
    finally:
        stop_spark_session()


def cmd_analyze_cdc(args: argparse.Namespace) -> None:
    print("Running Spark SQL diagnostic analytics...")
    from src.cdc.analysis import CDCAnalyzer
    from src.utils.spark import get_spark_session, stop_spark_session

    spark = get_spark_session()
    try:
        analyzer = CDCAnalyzer(spark=spark)
        diagnostics = analyzer.run_all_diagnostics()
        print("\n=== CDC Operations Breakdown ===")
        print(json.dumps(diagnostics["events_by_operation"], indent=2))

        print("\n=== Events by Source Table & Operation ===")
        print(json.dumps(diagnostics["events_by_source_table"], indent=2))

        print("\n=== Sequence Ranges by Batch ===")
        print(json.dumps(diagnostics["sequence_range_by_batch"], indent=2))

        print("\n=== Top Entities by Update Frequency ===")
        print(json.dumps(diagnostics["update_frequency_per_entity"], indent=2))

        print("\n=== Late Events by Table ===")
        print(json.dumps(diagnostics["late_events_by_table"], indent=2))

        print("\n=== Quarantine Rejection Reasons ===")
        print(json.dumps(diagnostics["invalid_events_by_reason"], indent=2))
    finally:
        stop_spark_session()


def cmd_run_all(args: argparse.Namespace) -> None:
    print("=== Executing Full Module 1 End-to-End Workflow ===")
    cmd_generate_snapshot(args)
    cmd_generate_cdc(args)
    cmd_process_cdc(args)
    cmd_analyze_cdc(args)
    print("\n✓ Full workflow execution completed successfully.")


def cmd_init_delta(args: argparse.Namespace) -> None:
    print("Initializing Delta current-state tables from initial snapshot...")
    from src.delta.current_state import initialize_delta_current_state
    from src.utils.spark import get_spark_session, stop_spark_session

    spark = get_spark_session()
    try:
        counts = initialize_delta_current_state(
            spark=spark,
            force_overwrite=args.force,
        )
        print("✓ Delta current-state tables initialized successfully:")
        for table, cnt in counts.items():
            print(f"  • {table}: {cnt} rows")
    finally:
        stop_spark_session()


def cmd_apply_cdc(args: argparse.Namespace) -> None:
    print("Executing Delta CDC Apply Pipeline...")
    from src.pipelines.delta_cdc_apply import DeltaCDCApplier
    from src.utils.spark import get_spark_session, stop_spark_session

    spark = get_spark_session()
    try:
        applier = DeltaCDCApplier(spark=spark)
        result = applier.run_apply_pipeline()

        if not result.batches_processed:
            print("✓ No new pending CDC batches. Delta current-state tables are up to date.")
            return

        print(f"✓ Successfully applied {len(result.batches_processed)} CDC batches to Delta Lake:")
        for b in result.batches_processed:
            print(f"  • Batch {b.batch_id}:")
            print(f"    - Valid unique events: {b.total_valid_unique_events}")
            print(f"    - Applied Inserts: {b.total_applied_inserts}")
            print(f"    - Applied Updates: {b.total_applied_updates}")
            print(f"    - Applied Deletes (Tombstones): {b.total_applied_deletes}")
            print(f"    - In-Batch Superseded Events: {b.total_superseded_events}")
            print(f"    - Stale No-Ops (seq <= HWM): {b.total_stale_noops}")
            print(f"    - Already Applied: {b.total_already_applied}")
            print(f"    - Recovered after crash: {b.total_recovered_events}")

        print("\n✓ Current Table High-Water Marks:")
        for t, hwm in result.checkpoint_state.items():
            print(f"  - {t}: sequence {hwm}")
    finally:
        stop_spark_session()


def cmd_show_current(args: argparse.Namespace) -> None:
    print(f"Querying Delta current-state table '{args.table}'...")
    from src.delta.current_state import load_active_table, load_current_table
    from src.utils.spark import get_spark_session, stop_spark_session

    spark = get_spark_session()
    try:
        if args.active_only:
            df = load_active_table(spark, args.table)
            print(f"=== Active Records in '{args.table}' (Total: {df.count()}) ===")
        else:
            df = load_current_table(spark, args.table)
            print(f"=== Physical Records in '{args.table}' (Total: {df.count()}) ===")
        df.show(args.limit, truncate=False)
    finally:
        stop_spark_session()


def cmd_show_applied_events(args: argparse.Namespace) -> None:
    print("Querying Applied Event Ledger (delta/audit/applied_events)...")
    from src.delta.audit import get_applied_events_path
    from src.utils.spark import get_spark_session, stop_spark_session

    spark = get_spark_session()
    try:
        p = get_applied_events_path()
        if not p.exists():
            print("No applied events ledger found.")
            return
        df = spark.read.format("delta").load(str(p))
        print(f"=== Applied Event Ledger (Total Records: {df.count()}) ===")
        df.orderBy(df["applied_at"].desc()).show(args.limit, truncate=False)
    finally:
        stop_spark_session()


def cmd_build_event_store(args: argparse.Namespace) -> None:
    print("Building canonical CDC Event Store from generated batches...")
    from src.events.event_store import backfill_event_store
    from src.utils.spark import get_spark_session, stop_spark_session

    spark = get_spark_session()
    try:
        count = backfill_event_store(spark)
        print(f"✓ Backfilled canonical CDC event store. Total events processed: {count}")
    finally:
        stop_spark_session()


def cmd_init_history(args: argparse.Namespace) -> None:
    print("Initializing subscriptions_history SCD2 table from snapshot...")
    from src.history.subscriptions_scd2 import initialize_subscriptions_history_from_snapshot
    from src.utils.spark import get_spark_session, stop_spark_session

    spark = get_spark_session()
    try:
        count = initialize_subscriptions_history_from_snapshot(spark, force_overwrite=args.force)
        print(f"✓ Seeded subscriptions_history with {count} baseline versions.")
    finally:
        stop_spark_session()


def cmd_detect_late_events(args: argparse.Namespace) -> None:
    print("Scanning for late CDC events and enqueuing to late_event_queue...")
    from pyspark.sql import functions as F

    from src.config.settings import DEFAULT_CDC_EVENT_STORE_DIR
    from src.delta.current_state import load_current_table
    from src.replay.queue import enqueue_late_events
    from src.utils.spark import get_spark_session, stop_spark_session

    spark = get_spark_session()
    try:
        store_df = spark.read.format("delta").load(str(DEFAULT_CDC_EVENT_STORE_DIR))
        sub_curr = load_current_table(spark, "subscriptions")

        joined = (
            store_df.filter(F.col("source_table") == "subscriptions")
            .join(
                sub_curr.select(
                    F.col("subscription_id").alias("curr_sub_id"),
                    F.col("_last_source_sequence").alias("curr_seq"),
                ),
                store_df.business_key == F.col("curr_sub_id"),
                "inner",
            )
            .filter(F.col("source_sequence") < F.col("curr_seq"))
        )

        enqueued = enqueue_late_events(spark, joined)
        print(f"✓ Detected and enqueued {enqueued} late events into replay queue.")
    finally:
        stop_spark_session()


def cmd_replay_late_events(args: argparse.Namespace) -> None:
    print("Processing pending late-event historical replays...")
    from src.replay.processor import process_pending_replays
    from src.utils.spark import get_spark_session, stop_spark_session

    spark = get_spark_session()
    try:
        summary = process_pending_replays(spark, repair_current=args.repair_current)
        print("✓ Replay execution summary:")
        print(f"  - Total pending evaluated: {summary['total_pending']}")
        print(f"  - Successfully applied: {summary['applied']}")
        print(f"  - Conflicts detected: {summary['conflict']}")
        print(f"  - Failures: {summary['failed']}")
    finally:
        stop_spark_session()


def cmd_show_history(args: argparse.Namespace) -> None:
    from pyspark.sql import functions as F

    from src.config.settings import DEFAULT_SUBSCRIPTIONS_HISTORY_DIR
    from src.utils.spark import get_spark_session, stop_spark_session

    spark = get_spark_session()
    try:
        hist_df = spark.read.format("delta").load(str(DEFAULT_SUBSCRIPTIONS_HISTORY_DIR))
        if args.subscription_id:
            hist_df = hist_df.filter(F.col("subscription_id") == args.subscription_id)

        hist_df = hist_df.orderBy(F.col("valid_from_sequence").asc())
        print(f"\n=== Subscriptions SCD2 History ({args.subscription_id or 'All'}) ===")
        hist_df.show(args.limit, truncate=False)
    finally:
        stop_spark_session()


def cmd_show_replay_queue(args: argparse.Namespace) -> None:
    from src.config.settings import DEFAULT_LATE_EVENT_QUEUE_DIR
    from src.utils.spark import get_spark_session, stop_spark_session

    spark = get_spark_session()
    try:
        queue_df = spark.read.format("delta").load(str(DEFAULT_LATE_EVENT_QUEUE_DIR))
        print("\n=== Late Event Replay Queue ===")
        queue_df.show(args.limit, truncate=False)
    finally:
        stop_spark_session()


def cmd_migrate_schema(args: argparse.Namespace) -> None:
    print(
        f"Running controlled schema migration for table '{args.table}' to version {args.to_version}..."
    )
    from src.schema_evolution.migrations import migrate_subscriptions_to_v2
    from src.utils.spark import get_spark_session, stop_spark_session

    spark = get_spark_session()
    try:
        if args.table == "subscriptions" and args.to_version == 2:
            res = migrate_subscriptions_to_v2(spark)
            print(f"✓ Migration result: {res['status']} - {res['message']}")
            print(f"  Columns: {res['columns']}")
        else:
            print(f"Error: Unsupported migration target {args.table} -> v{args.to_version}")
    finally:
        stop_spark_session()


def main() -> None:
    parser = argparse.ArgumentParser(description="Incremental CDC Databricks Pipeline CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # generate-snapshot
    p_snap = subparsers.add_parser("generate-snapshot", help="Generate initial OLTP state")
    p_snap.add_argument("--scale", choices=["tiny", "small", "standard"], default="tiny")
    p_snap.add_argument("--seed", type=int, default=42)

    # generate-cdc
    p_cdc = subparsers.add_parser("generate-cdc", help="Generate synthetic CDC batches")
    p_cdc.add_argument("--batches", type=int, default=3)
    p_cdc.add_argument("--scale", choices=["tiny", "small", "standard"], default="tiny")
    p_cdc.add_argument("--seed", type=int, default=42)
    p_cdc.add_argument(
        "--schema-transition-batch",
        type=int,
        default=None,
        help="Batch ID from which newly generated subscriptions use Schema V2",
    )

    # process-cdc (Module 1 change-feed staging)
    subparsers.add_parser(
        "process-cdc", help="Process pending CDC batches into valid/quarantine/late"
    )

    # analyze-cdc (Module 1 diagnostic views)
    subparsers.add_parser("analyze-cdc", help="Run Spark SQL diagnostics")

    # init-delta (Module 2 snapshot -> Delta)
    p_init_delta = subparsers.add_parser(
        "init-delta", help="Initialize Delta current-state tables from snapshot"
    )
    p_init_delta.add_argument(
        "--force", action="store_true", help="Force overwrite existing Delta tables"
    )

    # apply-cdc (Module 2 Delta MERGE apply)
    subparsers.add_parser(
        "apply-cdc", help="Apply CDC change feed into Delta current-state tables via MERGE"
    )

    # show-current (Module 2 inspect current state)
    p_show = subparsers.add_parser("show-current", help="Display current-state Delta table")
    p_show.add_argument(
        "--table",
        choices=["accounts", "subscriptions", "invoices", "payments"],
        default="subscriptions",
    )
    p_show.add_argument("--limit", type=int, default=20)
    p_show.add_argument(
        "--active-only", action="store_true", help="Filter out soft-deleted tombstones"
    )

    # show-applied-events (Module 2 inspect ledger)
    p_ledger = subparsers.add_parser(
        "show-applied-events", help="Display applied events audit ledger"
    )
    p_ledger.add_argument("--limit", type=int, default=20)

    # Module 3 Commands:
    # build-event-store
    subparsers.add_parser("build-event-store", help="Build canonical CDC event store from batches")

    # init-history
    p_hist_init = subparsers.add_parser(
        "init-history", help="Initialize subscriptions SCD2 history from snapshot"
    )
    p_hist_init.add_argument(
        "--force", action="store_true", help="Force overwrite existing history"
    )

    # detect-late-events
    subparsers.add_parser(
        "detect-late-events", help="Detect late events and enqueue to replay queue"
    )

    # replay-late-events
    p_replay = subparsers.add_parser(
        "replay-late-events", help="Process pending late event replays"
    )
    p_replay.add_argument(
        "--repair-current", action="store_true", help="Explicitly align current state on drift"
    )

    # show-history
    p_show_hist = subparsers.add_parser("show-history", help="Display subscriptions SCD2 history")
    p_show_hist.add_argument("--subscription-id", type=str, default=None)
    p_show_hist.add_argument("--limit", type=int, default=20)

    # show-replay-queue
    p_show_queue = subparsers.add_parser(
        "show-replay-queue", help="Display late event replay queue"
    )
    p_show_queue.add_argument("--limit", type=int, default=20)

    # migrate-schema
    p_mig = subparsers.add_parser(
        "migrate-schema", help="Run controlled Delta table schema migration"
    )
    p_mig.add_argument("--table", choices=["subscriptions"], default="subscriptions")
    p_mig.add_argument("--to-version", type=int, default=2)

    # run-all
    p_all = subparsers.add_parser("run-all", help="Run end-to-end pipeline")
    p_all.add_argument("--batches", type=int, default=3)
    p_all.add_argument("--scale", choices=["tiny", "small", "standard"], default="tiny")
    p_all.add_argument("--seed", type=int, default=42)

    args = parser.parse_args()

    commands = {
        "generate-snapshot": cmd_generate_snapshot,
        "generate-cdc": cmd_generate_cdc,
        "process-cdc": cmd_process_cdc,
        "analyze-cdc": cmd_analyze_cdc,
        "init-delta": cmd_init_delta,
        "apply-cdc": cmd_apply_cdc,
        "show-current": cmd_show_current,
        "show-applied-events": cmd_show_applied_events,
        "build-event-store": cmd_build_event_store,
        "init-history": cmd_init_history,
        "detect-late-events": cmd_detect_late_events,
        "replay-late-events": cmd_replay_late_events,
        "show-history": cmd_show_history,
        "show-replay-queue": cmd_show_replay_queue,
        "migrate-schema": cmd_migrate_schema,
        "run-all": cmd_run_all,
    }

    cmd_fn = commands.get(args.command)
    if cmd_fn:
        cmd_fn(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
