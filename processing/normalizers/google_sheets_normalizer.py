import pandas as pd
from config.settings import config
from config.logging import get_logger

logger = get_logger("GoogleSheetsNormalizer")


class GoogleSheetsNormalizer:
    """
    Normalize Google Sheets BP & HR data to canonical row-level schema.
    No aggregation happens here.
    """

    def __init__(self):
        # Uses centralized registry paths
        self.raw_path = config.raw_gs_path
        self.output_path = config.norm_bp_path

    def run(self) -> pd.DataFrame:
        logger.info("Starting Google Sheets row-level normalization")

        if not self.raw_path.exists():
            raise FileNotFoundError(f"Raw Google Sheets data not found at {self.raw_path}")

        df = pd.read_csv(self.raw_path)

        # --- Clean column names ---
        df.columns = df.columns.str.strip().str.lower()

        # --- Canonical column mapping (fuzzy-safe) ---
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
            raise ValueError(f"Missing expected columns after normalization: {missing}")

        # --- Keep only canonical columns & Type coercion ---
        df = df[list(expected_cols)].copy()
        for col in ["systolic", "diastolic", "pulse"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # --- Sort and Clean ---
        df = df.sort_values("datetime").reset_index(drop=True)

        # --- Write output ---
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(self.output_path, index=False)

        logger.info(f"Normalization complete: {len(df)} rows -> {self.output_path}")
        return df
