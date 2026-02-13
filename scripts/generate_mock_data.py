"""
Mock Data Generator.
Creates a synthetic version of 'merged_daily_metrics.csv' for public demos.
Preserves schema but randomizes values within realistic physiological ranges.
"""
import pandas as pd
import numpy as np
from pathlib import Path
from config.settings import config
from config.logging import setup_logging, get_logger

setup_logging()
logger = get_logger("mock_generator")

def generate_mock_data():
    # 1. Define range (1 year of data)
    dates = pd.date_range(start="2025-01-01", end="2025-12-31", freq="D")
    n = len(dates)

    # 2. Generate Synthetic Metrics (Deterministic Randomness)
    np.random.seed(42) 
    
    data = {
        "date": dates,
        # BP: Normal distribution around 120/80 with some noise
        "systolic": np.random.normal(120, 8, n).astype(int),
        "diastolic": np.random.normal(80, 6, n).astype(int),
        "pulse": np.random.normal(72, 5, n).astype(int),
        
        # Sleep: Normal distribution around 7 hours (420 mins)
        "total_duration": np.random.normal(420, 45, n).astype(int),
        "sleep_score": np.random.normal(85, 8, n).clip(0, 100).astype(int),
        
        # HR: Daily averages
        "avg_hr": np.random.normal(68, 4, n).astype(int),
        "min_hr": np.random.normal(55, 3, n).astype(int),
        "max_hr": np.random.normal(130, 15, n).astype(int),
        
        # Metadata: Simulate missing data (10% chance)
        "missing_data_flag": np.random.choice([True, False], n, p=[0.1, 0.9])
    }

    df = pd.DataFrame(data)

    # 3. Save to processed folder (Mock Artifact)
    # We save it alongside the real data but with a distinct name
    output_path = config.MERGED_DATA_DIR / "mock_daily_metrics.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    df.to_csv(output_path, index=False)
    logger.info(f"✅ Mock data generated: {output_path} ({len(df)} rows)")

if __name__ == "__main__":
    generate_mock_data()