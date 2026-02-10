from pathlib import Path
from dataclasses import dataclass, field
import os
from dotenv import load_dotenv

# Load environment variables from .env if present
load_dotenv()

@dataclass
class AppConfig:
    # --- Core ---
    ENV: str = os.getenv("ENV", "dev")  # dev | prod
    DEBUG: bool = os.getenv("DEBUG", "true").lower() == "true"

    # --- Paths ---
    BASE_DIR = Path(__file__).resolve().parent.parent
    DATA_DIR: Path = BASE_DIR / "data"
    RAW_DATA_DIR: Path = DATA_DIR / "raw"
    RAW_MI_BAND_DATA_DIR: Path = RAW_DATA_DIR / "mi_band"
    RAW_GOOGLE_SHEETS_DATA_DIR: Path = RAW_DATA_DIR / "google_sheets"
    PROCESSED_DATA_DIR: Path = DATA_DIR / "processed"
    NORMALIZED_DATA_DIR: Path = PROCESSED_DATA_DIR / "normalized"
    VALIDATED_DATA_DIR: Path = PROCESSED_DATA_DIR / "validated"
    MERGED_DATA_DIR: Path = PROCESSED_DATA_DIR / "merged"
    PIPELINE_STATE_DIR: Path = PROCESSED_DATA_DIR / "pipeline_state.json"

    # --- Raw Filenames ---
    FN_RAW_GS: str = "bp_hr_google_sheets.csv"

    # --- Normalized Filenames ---
    FN_NORM_BP: str = "bp_hr_normalized.csv"
    FN_NORM_HR: str = "hr_daily_normalized.csv"
    FN_NORM_SLEEP: str = "sleep_daily_normalized.csv"

    # --- Merged Filename ---
    FN_MERGED: str = "merged_daily_metrics.csv"

    # --- Validated Filenames ---
    FN_VAL_BP: str = "validated_bp_hr.csv"
    FN_VAL_HR: str = "validated_hr_daily.csv"
    FN_VAL_SLEEP: str = "validated_sleep_daily.csv"

    # --- Validation Config (Fixed using default_factory) ---
    COLUMNS_BP: list[str] = field(default_factory=lambda: ["datetime", "systolic", "diastolic", "pulse"])
    COLUMNS_HR: list[str] = field(default_factory=lambda: ["date_only", "avg_hr", "min_hr", "max_hr"])
    COLUMNS_SLEEP: list[str] = field(default_factory=lambda: ["date_only", "total_duration", "sleep_score"])

    # --- Computed Path Properties (Registry) ---
    @property
    def raw_gs_path(self) -> Path: return self.RAW_GOOGLE_SHEETS_DATA_DIR / self.FN_RAW_GS

    @property
    def norm_bp_path(self) -> Path: return self.NORMALIZED_DATA_DIR / self.FN_NORM_BP

    @property
    def norm_hr_path(self) -> Path: return self.NORMALIZED_DATA_DIR / self.FN_NORM_HR

    @property
    def norm_sleep_path(self) -> Path: return self.NORMALIZED_DATA_DIR / self.FN_NORM_SLEEP

    @property
    def val_bp_path(self) -> Path: return self.VALIDATED_DATA_DIR / self.FN_VAL_BP

    @property
    def val_hr_path(self) -> Path: return self.VALIDATED_DATA_DIR / self.FN_VAL_HR

    @property
    def val_sleep_path(self) -> Path: return self.VALIDATED_DATA_DIR / self.FN_VAL_SLEEP

    @property
    def merged_path(self) -> Path: return self.MERGED_DATA_DIR / self.FN_MERGED

    # --- Google API Scopes ---
    GOOGLE_API_SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]

    # --- Google Sheets ---
    GOOGLE_SHEETS_KEY: str = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
    GOOGLE_SHEET_NAME: str = os.getenv("GOOGLE_SHEET_NAME", "ADHD BP & HR")

    def __post_init__(self):
        """Validate critical paths after initialization."""
        if not self.GOOGLE_SHEETS_KEY:
            raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS not set in .env")

        key_path = Path(self.GOOGLE_SHEETS_KEY)
        if not key_path.exists():
            raise FileNotFoundError(f"Service account key not found at: {key_path.absolute()}")

    GOOGLE_SHEETS_BP_ID: str = os.getenv("GOOGLE_SHEETS_BP_ID", "")
    GOOGLE_SHEETS_HR_ID: str = os.getenv("GOOGLE_SHEETS_HR_ID", "")

    # --- Mi Band Google Drive ---
    MI_BAND_DRIVE_FOLDER_ID: str = os.getenv("MI_BAND_DRIVE_FOLDER_ID", "")

    # --- Medication tracking ---
    MED_START_DATE: str = os.getenv("MED_START_DATE", "2025-12-08")

    # --- Pipeline toggles ---
    INGESTION_ENABLED: bool = os.getenv("INGESTION_ENABLED", "true").lower() == "true"
    VALIDATION_ENABLED: bool = os.getenv("VALIDATION_ENABLED", "true").lower() == "true"
    PROCESSING_ENABLED: bool = os.getenv("PROCESSING_ENABLED", "true").lower() == "true"

    # --- API ---
    API_HOST: str = os.getenv("API_HOST", "0.0.0.0")
    API_PORT: int = int(os.getenv("API_PORT", "8000"))

    # --- Frontend ---
    FRONTEND_PORT: int = int(os.getenv("FRONTEND_PORT", "8501"))


# Singleton instance
config = AppConfig()
