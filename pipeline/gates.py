"""Pipeline gates to ensure data integrity between stages.

Gates act as preconditions for DAG nodes. They verify the presence of
expected input files before a stage is allowed to run, preventing
downstream failures caused by missing artifacts.
"""
from pathlib import Path
from config.logging import get_logger
from config.settings import config

logger = get_logger("gates")

def require_files(stage: str, paths: list[Path]) -> bool:
    """Generic gate: ensure required files exist before continuing.

    Args:
        stage: Name of the stage that requires the input files.
        paths: List of paths that must exist for the gate to pass.

    Returns:
        True if all files exist; False otherwise (and logs an error).
    """
    missing = [p for p in paths if not p.exists()]

    if missing:
        logger.error(
            f"Gate failed before '{stage}'. Missing files:\n"
            + "\n".join(f"- {p}" for p in missing)
        )
        return False

    logger.info(f"Gate passed for '{stage}'")
    return True

def ingestion_gate() -> bool:
    """Verify raw files exist before normalization.

    Ensures ingestion has successfully produced raw CSVs for both Google
    Sheets and Mi Band sources before running the normalization stage.
    """
    return require_files(
        stage="normalization",
        paths=[
            config.raw_gs_path,  # Fixed: matches settings.py property
            config.RAW_MI_BAND_DATA_DIR,
        ],
    )

def normalization_gate() -> bool:
    """Verify normalized files exist before validation.

    Ensures the normalization stage has produced all expected normalized
    datasets before running validators.
    """
    return require_files(
        stage="validation",
        paths=[
            config.norm_bp_path,
            config.norm_hr_path,
            config.norm_sleep_path,
        ],
    )

def validation_gate() -> bool:
    """Verify validated files exist before merging.

    Ensures all validated datasets are present before computing merged
    daily metrics, protecting against partial or failed validation runs.
    """
    return require_files(
        stage="merge_daily_metrics",
        paths=[
            config.val_bp_path,
            config.val_hr_path,
            config.val_sleep_path,
        ],
    )
