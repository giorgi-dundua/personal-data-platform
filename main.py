# main.py
from config.logging import setup_logging, get_logger
from config.settings import config
from ingestion.google_sheets_source import GoogleSheetsSource
from ingestion.mi_band_drive_source import MiBandDriveSource
from ingestion.pipeline import IngestionPipeline
from processing.normalizers.google_sheets_normalizer import GoogleSheetsNormalizer
from processing.normalizers.mi_band_normalizer import MiBandNormalizer
from processing.validators.validate import Validator
from processing.aggregators.merge_daily_metrics import merge_daily_metrics

# ---- Setup logging ----
setup_logging()
logger = get_logger("bootstrap")
logger.info(f"Application starting | ENV={config.ENV}, DEBUG={config.DEBUG}")

# ---- Ingestion ----
sources = [
    GoogleSheetsSource(sheet_name="ADHD BP & HR", csv_path=config.RAW_GOOGLE_SHEETS_DATA_DIR / "bp_hr_google_sheets.csv"),
    MiBandDriveSource(drive_folder_id=config.MI_BAND_DRIVE_FOLDER_ID, raw_dir=config.RAW_MI_BAND_DATA_DIR)
]
pipeline = IngestionPipeline(sources)
logger.info("Starting ingestion pipeline...")
pipeline.run()
logger.info("Ingestion pipeline complete.")

# ---- Normalization ----
logger.info("Starting normalization...")

gs_normalizer = GoogleSheetsNormalizer()  # paths handled internally
gs_df = gs_normalizer.run()

mi_normalizer = MiBandNormalizer()
mi_df_sleep, mi_df_hr = mi_normalizer.run()

logger.info("Normalization complete.")

# ---- Validation ----
logger.info("Starting validation...")

# BP validator
bp_validator = Validator(
    input_csv=config.NORMALIZED_DATA_DIR / "bp_hr_normalized.csv",
    output_csv=config.VALIDATED_DATA_DIR / "validated_bp_hr.csv",
    required_columns=["datetime","systolic","diastolic","pulse"],
    date_col="datetime"
)
bp_validator.run()

# HR validator
hr_validator = Validator(
    input_csv=config.NORMALIZED_DATA_DIR / "hr_daily_normalized.csv",
    output_csv=config.VALIDATED_DATA_DIR / "validated_hr_daily.csv",
    required_columns=["date_only","avg_hr","min_hr","max_hr"],
    date_col="date_only"
)
hr_validator.run()

# Sleep validator
sleep_validator = Validator(
    input_csv=config.NORMALIZED_DATA_DIR / "sleep_daily_normalized.csv",
    output_csv=config.VALIDATED_DATA_DIR / "validated_sleep_daily.csv",
    required_columns=["date_only","total_duration","sleep_score"],
    date_col="date_only"
)
sleep_validator.run()

logger.info("Validation complete.")

# ---- Merge daily metrics ----
merge_daily_metrics(
    bp_csv=config.VALIDATED_DATA_DIR / "validated_bp_hr.csv",
    hr_csv=config.VALIDATED_DATA_DIR / "validated_hr_daily.csv",
    sleep_csv=config.VALIDATED_DATA_DIR / "validated_sleep_daily.csv",
    output_csv=config.MERGED_DATA_DIR / "merged_daily_metrics.csv",
)

logger.info("All processing complete. Merged daily metrics ready.")
