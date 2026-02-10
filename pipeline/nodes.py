"""
Pipeline Node Wrappers.
Each function encapsulates a DAG stage and returns its metadata and artifact paths.
"""
from typing import Any, Dict
from ingestion.runner import IngestionRunner
from ingestion.google_sheets_source import GoogleSheetsSource
from ingestion.mi_band_drive_source import MiBandDriveSource
from processing.normalizers.google_sheets_normalizer import GoogleSheetsNormalizer
from processing.normalizers.mi_band_normalizer import MiBandNormalizer
from processing.validators.validate import Validator
from processing.aggregators.merge_daily_metrics import merge_daily_metrics
from config.settings import config


def ingestion_stage() -> Dict[str, Any]:
    """
    Ingestion stage: download raw data and return temporary paths.
    """
    sources = [
        GoogleSheetsSource(),
        MiBandDriveSource(),
    ]
    # Runner now returns List[Path]
    paths = IngestionRunner(sources).run()

    return {
        "metrics": {"files_ingested": len(paths)},
        "artifacts": {
            "raw_data": paths
        }
    }


def normalization_stage() -> Dict[str, Any]:
    """
    Runs all normalizers and returns paths to temporary artifacts.
    """
    gs_df, gs_path = GoogleSheetsNormalizer().run()
    mi_sleep, mi_hr, mi_paths = MiBandNormalizer().run()

    return {
        "metrics": {
            "google_sheets_rows": len(gs_df),
            "mi_band_sleep_rows": len(mi_sleep),
        },
        "artifacts": {
            "normalized_data": [gs_path] + mi_paths
        }
    }


def validation_stage() -> Dict[str, Any]:
    """Runs validators and returns paths to temporary artifacts."""
    bp_df, bp_path = Validator(config.norm_bp_path, config.val_bp_path, config.COLUMNS_BP, "datetime").run()
    hr_df, hr_path = Validator(config.norm_hr_path, config.val_hr_path, config.COLUMNS_HR, "date_only").run()
    sleep_df, sleep_path = Validator(config.norm_sleep_path, config.val_sleep_path, config.COLUMNS_SLEEP, "date_only").run()

    return {
        "metrics": {"bp_rows": len(bp_df), "hr_rows": len(hr_df), "sleep_rows": len(sleep_df)},
        "artifacts": {
            "validated_data": [bp_path, hr_path, sleep_path]
        }
    }


def merge_stage() -> Dict[str, Any]:
    """Runs merge and returns path to temporary artifact."""
    df, path = merge_daily_metrics()
    return {
        "metrics": {"merged_rows": len(df)},
        "artifacts": {
            "daily_metrics": [path]
        }
    }