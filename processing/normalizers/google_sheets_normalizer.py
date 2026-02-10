import uuid
import pandas as pd
from pathlib import Path
from config.settings import config
from config.logging import get_logger

logger = get_logger("GoogleSheetsNormalizer")


class GoogleSheetsNormalizer:
    """
    Normalize Google Sheets BP & HR data to canonical row-level schema.
    """

    def __init__(self):
        self.raw_path = config.raw_gs_path
        self.output_path = config.norm_bp_path

    def run(self) -> tuple[pd.DataFrame, Path]:
        """
        Normalize Google Sheets data and write to a unique temporary file.
        Returns: (DataFrame, Path to temp file)
        """
        logger.info("Starting Google Sheets row-level normalization")

        if not self.raw_path.exists():
            raise FileNotFoundError(f"Raw Google Sheets data not found at {self.raw_path}")

        df = pd.read_csv(self.raw_path)

        # --- Clean column names ---
        df.columns = df.columns.str.strip().str.lower()

        # --- Canonical column mapping ---
        column_map = {
            "date": "date",
            "time": "time",
            "systolic": "systolic",
            "diastolic": "diastolic",
            "pulse": "pulse",
        }

        normalized_cols = {}
        for col, canonical in column_map.items():
            matches = [c for c in df.columns if c.replace(" ", "") == col.replace(" ", "")]
            if matches:
                normalized_cols[matches[0]] = canonical
        df = df.rename(columns=normalized_cols)

        # --- Build datetime ---
        if "datetime" not in df.columns:
            df["datetime"] = pd.to_datetime(
                df["date"].astype(str) + " " + df["time"].astype(str),
                errors="coerce"
            )

        # --- Verify expected columns ---
        expected_cols = {"datetime", "systolic", "diastolic", "pulse"}
        missing = expected_cols - set(df.columns)
        if missing:
            raise ValueError(f"Missing expected columns: {missing}")

        # --- Keep only canonical columns & Type coercion ---
        df = df[list(expected_cols)].copy()
        for col in ["systolic", "diastolic", "pulse"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # --- Sort and Clean ---
        df = df.sort_values("datetime").reset_index(drop=True)

        # --- Atomic Write ---
        unique_id = uuid.uuid4().hex[:4]
        tmp_path = self.output_path.with_suffix(f".csv.{unique_id}.tmp")

        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(tmp_path, index=False)

        logger.info(f"Normalization complete: {len(df)} rows -> {tmp_path.name}")
        return df, tmp_path