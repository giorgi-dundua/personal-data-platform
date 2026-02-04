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
    parser.add_argument(
        "--skip-ingestion",
        action="store_true",
        help="Skip downloading new data and use existing local raw files",
    )
    return parser.parse_args()

def main():
    """Main execution block."""
    args = parse_args()

    # Trigger cleanup if either --clean or --dry-run is present
    if args.clean or args.dry_run:
        logger.info("Cleanup/Dry-run process initiated...")
        # We include raw files if --raw is used OR if we are just doing a --dry-run preview
        include_raw = args.raw or args.dry_run
        clean_project_data(dry_run=args.dry_run, include_raw=include_raw)

    # Do not run the pipeline if we are only doing a dry run
    if args.dry_run:
        logger.info("Dry run complete. Pipeline execution skipped.")
        return

    try:
        # Pass the skip_ingestion flag to the orchestrator
        run_pipeline(skip_ingestion=args.skip_ingestion)
    except Exception as e:
        logger.critical(f"Pipeline crashed: {e}", exc_info=True)

if __name__ == "__main__":
    main()
