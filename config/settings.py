from pathlib import Path
from dataclasses import dataclass
import os
from dotenv import load_dotenv

# Load environment variables from .env if present
load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent


@dataclass
class AppConfig:
    # --- Core ---
    ENV: str = os.getenv("ENV", "dev")  # dev | prod
    DEBUG: bool = os.getenv("DEBUG", "true").lower() == "true"

    # --- Paths ---
    DATA_DIR: Path = BASE_DIR / "data"
    RAW_DATA_DIR: Path = DATA_DIR / "raw"
    RAW_MI_BAND_DATA_DIR: Path = RAW_DATA_DIR / "mi_band"
    RAW_GOOGLE_SHEETS_DATA_DIR: Path = RAW_DATA_DIR / "google_sheets"
    PROCESSED_DATA_DIR: Path = DATA_DIR / "processed"
    NORMALIZED_DATA_DIR: Path = PROCESSED_DATA_DIR / "normalized"
    VALIDATED_DATA_DIR: Path = PROCESSED_DATA_DIR / "validated"
    MERGED_DATA_DIR: Path = PROCESSED_DATA_DIR / "merged"


    # --- Google Sheets ---
    GOOGLE_SHEETS_KEY: str = os.getenv(
        "GOOGLE_SHEETS_KEY",
        BASE_DIR / "config" / "personal-data-platform-486113-d64b62f4c09a.json"
    )
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
