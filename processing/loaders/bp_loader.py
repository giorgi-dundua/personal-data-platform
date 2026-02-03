"""
Load validated BP data using canonical AppConfig paths.
"""
import pandas as pd
from config.settings import config
from config.logging import get_logger

logger = get_logger("bp_loader")


def load_bp_data():
    """
    Load and normalize blood pressure data into canonical schema.
    Returns a DataFrame and row count for pipeline state.
    """
    input_file = config.val_bp_path

    if not input_file.exists():
        raise FileNotFoundError(f"Validated BP file not found: {input_file}")

    df = pd.read_csv(input_file)

    # Canonical column mapping
    column_map = {
        "date": "date",
        "systolic": "systolic",
        "diastolic": "diastolic",
        "pulse": "pulse",
    }
    df = df[list(column_map.keys())].rename(columns=column_map)

    # ---- Types ----
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["systolic"] = pd.to_numeric(df["systolic"], errors="coerce").astype("Int64")
    df["diastolic"] = pd.to_numeric(df["diastolic"], errors="coerce").astype("Int64")
    df["pulse"] = pd.to_numeric(df["pulse"], errors="coerce").astype("Int64")

    # ---- Source tagging ----
    df["source"] = "google_sheets"

    # ---- Drop invalid rows ----
    df = df.dropna(subset=["date", "systolic", "diastolic"])
    logger.info(f"Loaded BP data with {len(df)} valid rows")

    return df, len(df)


if __name__ == "__main__":
    df, count = load_bp_data()
    print(df.head())
    print(f"Row count: {count}")
