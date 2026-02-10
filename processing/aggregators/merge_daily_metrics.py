import uuid
import pandas as pd
from pathlib import Path
from typing import Tuple
from config.settings import config
from config.logging import get_logger

logger = get_logger("merge_daily_metrics")


def merge_daily_metrics() -> Tuple[pd.DataFrame, Path]:
    """
    Merge validated BP, HR, and sleep daily metrics.
    Returns: (MergedDF, TempPath)
    """
    logger.info("Starting daily metrics merge")

    # 1. Load validated datasets
    bp_df = pd.read_csv(config.val_bp_path, parse_dates=["datetime"])
    hr_df = pd.read_csv(config.val_hr_path, parse_dates=["date_only"])
    sleep_df = pd.read_csv(config.val_sleep_path, parse_dates=["date_only"])

    # 2. Prepare BP for daily merge
    bp_df["date"] = bp_df["datetime"].dt.normalize()
    bp_daily = bp_df.groupby("date").mean(numeric_only=True).reset_index()

    # 3. Normalize join keys
    hr_df = hr_df.rename(columns={"date_only": "date"})
    sleep_df = sleep_df.rename(columns={"date_only": "date"})

    # 4. Perform Outer Join
    merged = bp_daily.merge(hr_df, on="date", how="outer")
    merged = merged.merge(sleep_df, on="date", how="outer")
    merged["missing_data_flag"] = merged.isna().any(axis=1)

    # 5. Atomic Write
    unique_id = uuid.uuid4().hex[:4]
    tmp_path = config.merged_path.with_suffix(f".csv.{unique_id}.tmp")

    config.merged_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(tmp_path, index=False)

    logger.info(f"Merge successful: {tmp_path.name} ({len(merged)} rows)")
    return merged, tmp_path