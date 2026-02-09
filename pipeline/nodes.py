"""Pipeline node wrapper functions.

Each function in this module represents a DAG node and encapsulates the
end-to-end work for a logical pipeline stage (ingestion, normalization,
validation, merge). The orchestrator invokes these functions based on the
topologically sorted DAG.
"""

from ingestion.google_sheets_source import GoogleSheetsSource
from ingestion.mi_band_drive_source import MiBandDriveSource
from ingestion.runner import IngestionRunner

from processing.normalizers.google_sheets_normalizer import GoogleSheetsNormalizer
from processing.normalizers.mi_band_normalizer import MiBandNormalizer

from processing.validators.validate import Validator
from processing.aggregators.merge_daily_metrics import merge_daily_metrics

from config.settings import config


# ---- DAG Node Wrappers ----

def ingestion_stage() -> bool:
    """Ingestion stage: download raw data from external sources.

    Orchestrates all configured ingestion sources (Google Sheets, Mi Band)
    via the ``IngestionRunner``. Returns True on success so the orchestrator
    can treat it as a simple passed/failed stage.
    """
    sources = [
        GoogleSheetsSource(sheet_name="ADHD BP & HR"),
        MiBandDriveSource(),
    ]
    IngestionRunner(sources).run()
    return True


def normalization_stage() -> dict[str, int]:
    """Normalization stage: produce canonical DataFrames from raw sources.

    Returns:
        Mapping from source name to row counts for the normalized outputs.
        Used for pipeline state reporting and basic sanity checks.
    """
    gs_df = GoogleSheetsNormalizer().run()
    mi_sleep, mi_hr = MiBandNormalizer().run()
    return {
        "google_sheets": len(gs_df),
        "mi_band_sleep": len(mi_sleep),
        "mi_band_hr": len(mi_hr),
    }


def validation_stage() -> dict[str, int]:
    """Validation stage: enforce schema and basic data quality rules.

    Runs validation for BP, HR, and sleep datasets and writes validated
    CSVs to the configured output paths.

    Returns:
        Mapping from dataset name (\"bp\", \"hr\", \"sleep\") to row counts
        after validation.
    """
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

    return {
        "bp": bp_count,
        "hr": hr_count,
        "sleep": sleep_count,
    }


def merge_stage() -> int:
    """Merge stage: build the aggregated daily metrics dataset.

    Uses validated BP, HR, and sleep inputs to create a merged daily
    metrics CSV and returns the number of rows produced.

    Returns:
        Row count of the merged daily metrics DataFrame.
    """
    merged_df, merged_count = merge_daily_metrics()
    return merged_count
