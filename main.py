"""
Main entry point for the Personal Data Platform pipeline.
Orchestrates ingestion, normalization, validation, and merging with gates.
Supports optional cleanup of generated artifacts via CLI arguments.
"""

import argparse
from config.logging import setup_logging, get_logger
from config.settings import config
import pandas as pd

# Ingestion
from ingestion.google_sheets_source import GoogleSheetsSource
from ingestion.mi_band_drive_source import MiBandDriveSource
from ingestion.pipeline import IngestionPipeline

# Processing
from processing.normalizers.google_sheets_normalizer import GoogleSheetsNormalizer
from processing.normalizers.mi_band_normalizer import MiBandNormalizer
from processing.validators.validate import Validator
from processing.aggregators.merge_daily_metrics import merge_daily_metrics

# Gates & State
from pipeline.gates import ingestion_gate, normalization_gate, validation_gate
from pipeline.pipeline_state import PipelineState

# Cleanup Utility
from scripts.cleanup import clean_project_data

# Initialize system logging
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
    return parser.parse_args()

def run_pipeline():
    """Executes the end-to-end data processing workflow."""

    logger.info(f"Application starting | ENV={config.ENV}, DEBUG={config.DEBUG}")

    # Initialize pipeline state
    state = PipelineState()

    # Pre-declare variables to avoid linter warnings / reference errors
    bp_df = hr_df = sleep_df = pd.DataFrame()
    source_rows = validated_sources = {}

    # ---- 1. Ingestion ----
    if config.INGESTION_ENABLED:
        logger.info("Starting ingestion pipeline...")
        sources = [
            GoogleSheetsSource(sheet_name="ADHD BP & HR"),
            MiBandDriveSource(),
        ]
        IngestionPipeline(sources).run()
    else:
        logger.warning("Ingestion disabled; skipping.")

    # Gate check
    gate_passed = ingestion_gate()
    state.update_stage("ingestion", gate_passed=gate_passed)
    if not gate_passed:
        logger.critical("Pipeline stopped: ingestion gate failed.")
        return

    # ---- 2. Normalization ----
    if config.PROCESSING_ENABLED:
        logger.info("Starting normalization...")
        gs_df = GoogleSheetsNormalizer().run()
        mi_sleep, mi_hr = MiBandNormalizer().run()

        source_rows = {
            "google_sheets": len(gs_df),
            "mi_band_sleep": len(mi_sleep),
            "mi_band_hr": len(mi_hr),
        }

        total_rows = sum(source_rows.values())
        logger.info("Normalization complete.")
    else:
        total_rows = 0
        logger.warning("Processing disabled; skipping normalization.")

    # Gate check
    gate_passed = normalization_gate()
    state.update_stage(
        "normalization",
        rows=total_rows,
        gate_passed=gate_passed,
        sources=source_rows,
    )
    if not gate_passed:
        logger.critical("Pipeline stopped: normalization gate failed.")
        return

    # ---- 3. Validation ----
    if config.VALIDATION_ENABLED:
        logger.info("Starting validation...")

        bp_df, bp_count = Validator(
            input_path=config.norm_bp_path,
            output_path=config.val_bp_path,
            required_columns=config.COLUMNS_BP,
            date_col="datetime"
        ).run_with_count()

        hr_df, hr_count = Validator(
            input_path=config.norm_hr_path,
            output_path=config.val_hr_path,
            required_columns=config.COLUMNS_HR,
            date_col="date_only"
        ).run_with_count()

        sleep_df, sleep_count = Validator(
            input_path=config.norm_sleep_path,
            output_path=config.val_sleep_path,
            required_columns=config.COLUMNS_SLEEP,
            date_col="date_only"
        ).run_with_count()

        total_validated = bp_count + hr_count + sleep_count

        validated_sources = {
            "bp": bp_count,
            "hr": hr_count,
            "sleep": sleep_count,
        }

        logger.info("Validation complete.")
    else:
        total_validated = 0
        logger.warning("Validation disabled; skipping.")

    # Gate check
    gate_passed = validation_gate()
    state.update_stage(
        "validation",
        rows=total_validated,
        gate_passed=gate_passed,
        sources=validated_sources,
    )
    if not gate_passed:
        logger.critical("Pipeline stopped: validation gate failed.")
        return

    # ---- 4. Merge Daily Metrics ----
    if config.PROCESSING_ENABLED:
        merged_df, merged_count = merge_daily_metrics()

        merge_sources = {
            "bp": len(bp_df),
            "hr": len(hr_df),
            "sleep": len(sleep_df),
        }

        state.update_stage(
            "merge",
            rows=merged_count,
            gate_passed=True,
            sources=merge_sources,
        )

        logger.info("Pipeline complete. Daily metrics ready.")

if __name__ == "__main__":
    args = parse_args()

    # Handle Cleanup logic
    if args.clean:
        logger.info("Cleanup flag detected. Removing old artifacts...")
        clean_project_data()

    try:
        run_pipeline()
    except Exception as e:
        logger.critical(f"Pipeline crashed: {e}", exc_info=True)
