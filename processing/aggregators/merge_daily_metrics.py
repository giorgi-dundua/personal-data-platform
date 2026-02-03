# processing/aggregators/merge_daily_metrics.py
from pathlib import Path
import pandas as pd
from config.logging import get_logger

logger = get_logger("merge_daily_metrics")


def merge_daily_metrics(
    bp_csv: Path,
    hr_csv: Path,
    sleep_csv: Path,
    output_csv: Path,
) -> pd.DataFrame:
    """
    Merge validated BP, HR, and sleep daily metrics into one dataset.
    """

    logger.info("Starting daily metrics merge")

    bp_df = pd.read_csv(bp_csv, parse_dates=["datetime"])
    bp_df["date"] = bp_df["datetime"].dt.normalize()
    bp_daily = bp_df.groupby("date").mean(numeric_only=True).reset_index()

    hr_df = pd.read_csv(hr_csv, parse_dates=["date_only"])
    hr_df = hr_df.rename(columns={"date_only": "date"})

    sleep_df = pd.read_csv(sleep_csv, parse_dates=["date_only"])
    sleep_df = sleep_df.rename(columns={"date_only": "date"})

    merged = bp_daily.merge(hr_df, on="date", how="outer")
    merged = merged.merge(sleep_df, on="date", how="outer")

    merged["missing_data_flag"] = merged.isna().any(axis=1)

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(output_csv, index=False)

    logger.info(f"Merged CSV saved to {output_csv} ({len(merged)} rows)")
    return merged
