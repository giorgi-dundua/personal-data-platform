from pathlib import Path
import pandas as pd
from config.logging import get_logger

logger = get_logger("Validator")

class Validator:
    """
    Generic validator for any row-level CSV dataset.
    required_columns: list of columns that are expected; missing columns are created as NA
    date_col: name of the column to use as datetime
    """

    def __init__(self, input_csv: Path, output_csv: Path, required_columns: list[str], date_col: str):
        self.input_csv = input_csv
        self.output_csv = output_csv
        self.required_columns = required_columns
        self.date_col = date_col
        if not self.output_csv.parent.exists():
            self.output_csv.parent.mkdir(parents=True, exist_ok=True)

    def load_data(self) -> pd.DataFrame:
        logger.info(f"Loading raw data from {self.input_csv}")
        df = pd.read_csv(self.input_csv, parse_dates=[self.date_col])
        logger.info(f"Loaded {len(df)} rows")
        return df

    def ensure_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in self.required_columns:
            if col not in df.columns:
                logger.warning(f"{col} column missing, creating empty column")
                df[col] = pd.NA
        return df

    def drop_missing_and_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        # Only drop missing for required columns that exist
        subset_cols = [c for c in self.required_columns if c in df.columns]
        before = len(df)
        df = df.dropna(subset=subset_cols)
        dropped = before - len(df)
        if dropped:
            logger.warning(f"Dropped {dropped} rows with missing critical values")
        # Drop exact duplicates
        before = len(df)
        df = df.drop_duplicates(subset=subset_cols)
        duplicates_dropped = before - len(df)
        if duplicates_dropped:
            logger.info(f"Dropped {duplicates_dropped} duplicate rows")
        return df

    def run(self) -> pd.DataFrame:
        logger.info("Validation started")
        df = self.load_data()
        df = self.ensure_columns(df)
        df = self.drop_missing_and_duplicates(df)
        df.to_csv(self.output_csv, index=False)
        logger.info(f"Validation complete. Validated data saved at {self.output_csv}, total rows: {len(df)}")
        return df
