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

    # process-cdc
    subparsers.add_parser("process-cdc", help="Process pending CDC batches incrementally")

    # analyze-cdc
    subparsers.add_parser("analyze-cdc", help="Run Spark SQL diagnostics")

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
