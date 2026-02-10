"""
Main entry point for the Personal Data Platform pipeline.
Handles CLI arguments, environment setup, and triggers the orchestrator.
"""
import argparse
from config.logging import setup_logging, get_logger
from scripts.cleanup import clean_project_data
from pipeline.orchestrator import run_pipeline

# Setup logging once at the very start
setup_logging()
logger = get_logger("bootstrap")

def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the pipeline."""
    parser = argparse.ArgumentParser(description="Run Personal Data Platform Pipeline")

    # Cleanup Arguments
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Remove generated CSV files before running the pipeline",
    )
    parser.add_argument(
        "--raw",
        action="store_true",
        help="Also delete raw downloads (use with --clean)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview which files would be deleted without actually removing them",
    )

    # Execution Control Arguments
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume pipeline from last successful stage using pipeline_state.json",
    )
    parser.add_argument(
        "--start-stage",
        type=str,
        choices=["ingestion", "normalization", "validation", "merge"],
        help="Explicitly start the pipeline from this stage (skips previous stages)",
    )

    return parser.parse_args()

def main():
    """Main execution block."""
    args = parse_args()

    # 1. Handle Cleanup
    if args.clean or args.dry_run:
        logger.info("Cleanup/Dry-run process initiated...")
        include_raw = args.raw or args.dry_run
        clean_project_data(dry_run=args.dry_run, include_raw=include_raw)

    if args.dry_run:
        logger.info("Dry run complete. Pipeline execution skipped.")
        return

    # 2. Trigger Pipeline
    try:
        run_pipeline(
            resume=args.resume,
            start_stage=args.start_stage
        )
    except Exception as e:
        logger.critical(f"Pipeline crashed: {e}", exc_info=True)

if __name__ == "__main__":
    main()