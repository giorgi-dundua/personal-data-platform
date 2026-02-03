"""
Main entry point for the Personal Data Platform pipeline.
Orchestrates Ingestion, Normalization, Validation, and Merging.
"""
from config.logging import setup_logging, get_logger
from config.settings import config

# Ingestion
from ingestion.google_sheets_source import GoogleSheetsSource
from ingestion.mi_band_drive_source import MiBandDriveSource
from ingestion.pipeline import IngestionPipeline

# Processing
from processing.normalizers.google_sheets_normalizer import GoogleSheetsNormalizer
from processing.normalizers.mi_band_normalizer import MiBandNormalizer
from processing.validators.validate import Validator
from processing.aggregators.merge_daily_metrics import merge_daily_metrics

# ---- Setup logging ----
setup_logging()
logger = get_logger("bootstrap")
logger.info(f"Application starting | ENV={config.ENV}, DEBUG={config.DEBUG}")


def run_pipeline():
    # ---- 1. Ingestion ----
    # Sources now pull their own IDs and paths from config internally
    sources = [
        GoogleSheetsSource(sheet_name="ADHD BP & HR"),
        MiBandDriveSource()
    ]

    if config.INGESTION_ENABLED:
        logger.info("Starting ingestion pipeline...")
        IngestionPipeline(sources).run()
    else:
        logger.warning("Ingestion is disabled in config; skipping.")

    # ---- 2. Normalization ----
    if config.PROCESSING_ENABLED:
        logger.info("Starting normalization...")

        # Paths are handled internally via config registry
        GoogleSheetsNormalizer().run()
        MiBandNormalizer().run()

        logger.info("Normalization complete.")

    # ---- 3. Validation ----
    if config.VALIDATION_ENABLED:
        logger.info("Starting validation...")

        # BP Validation
        Validator(
            input_path=config.norm_bp_path,
            output_path=config.val_bp_path,
            required_columns=config.COLUMNS_BP,
            date_col="datetime"
        ).run()

        # HR Validation
        Validator(
            input_path=config.norm_hr_path,
            output_path=config.val_hr_path,
            required_columns=config.COLUMNS_HR,
            date_col="date_only"
        ).run()

        # Sleep Validation
        Validator(
            input_path=config.norm_sleep_path,
            output_path=config.val_sleep_path,
            required_columns=config.COLUMNS_SLEEP,
            date_col="date_only"
        ).run()

        logger.info("Validation complete.")

    # ---- 4. Merge Daily Metrics ----
    if config.PROCESSING_ENABLED:
        # Aggregator now pulls all paths directly from config registry
        merge_daily_metrics()
        logger.info("All processing complete. Merged daily metrics ready.")


if __name__ == "__main__":
    try:
        run_pipeline()
    except Exception as e:
        logger.critical(f"Pipeline crashed: {e}", exc_info=True)
