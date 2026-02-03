import pandas as pd
from pathlib import Path


PROCESSED_DIR = Path(__file__).resolve().parents[2] / "data" / "processed"
INPUT_FILE = PROCESSED_DIR / "validated_bp_hr.csv"


def load_bp_data() -> pd.DataFrame:
    """
    Load and normalize blood pressure data into canonical schema.

    Canonical schema:
        date: datetime64
        systolic: int
        diastolic: int
        pulse: int
        source: str
    """

    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Validated BP file not found: {INPUT_FILE}")

    df = pd.read_csv(INPUT_FILE)

    # column mapping
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

    return df


if __name__ == "__main__":

    df = load_bp_data()
    print(df.head())
    print("\nSchema:")
    print(df.dtypes)
