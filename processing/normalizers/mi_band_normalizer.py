import pandas as pd
import json
from config.settings import config
from config.logging import get_logger

logger = get_logger("MiBandNormalizer")


class MiBandNormalizer:
    """
    Normalize Mi Band raw CSVs into daily HR and sleep metrics.
    Saves normalized files to data/normalized/.
    """

    def __init__(self):
        self.raw_dir = config.RAW_MI_BAND_DATA_DIR
        self.normalized_dir = config.NORMALIZED_DATA_DIR
        self.normalized_dir.mkdir(parents=True, exist_ok=True)

    def load_raw_files(self):
        files = list(self.raw_dir.glob("*.csv"))
        if not files:
            raise FileNotFoundError(f"No raw Mi Band CSVs found in {self.raw_dir}")
        dfs = [pd.read_csv(f) for f in files]
        logger.info(f"Loaded {len(files)} raw CSVs, total rows: {sum(len(df) for df in dfs)}")
        return pd.concat(dfs, ignore_index=True)

    def normalize_sleep(self, raw_df: pd.DataFrame):
        sleep_df = raw_df[raw_df['Key'] == 'sleep'].copy()
        sleep_df['Time'] = pd.to_datetime(sleep_df['Time'], unit='s')

        # Flatten JSON metrics
        sleep_dicts = sleep_df['Value'].apply(json.loads).tolist()
        sleep_norm = pd.json_normalize(sleep_dicts)

        sleep_df = sleep_df.reset_index(drop=True)
        sleep_final = pd.concat([sleep_df[['Time']], sleep_norm], axis=1)

        # Daily aggregation
        sleep_features = sleep_final[['Time',
                                     'total_duration',
                                     'sleep_deep_duration',
                                     'sleep_light_duration',
                                     'sleep_rem_duration',
                                     'sleep_awake_duration',
                                     'sleep_score',
                                     'has_data']].copy()
        sleep_features['date_only'] = sleep_features['Time'].dt.normalize()

        sleep_daily = sleep_features.groupby('date_only').agg(
            total_duration=('total_duration', 'mean'),
            sleep_deep_duration=('sleep_deep_duration', 'mean'),
            sleep_light_duration=('sleep_light_duration', 'mean'),
            sleep_rem_duration=('sleep_rem_duration', 'mean'),
            sleep_awake_duration=('sleep_awake_duration', 'mean'),
            sleep_score=('sleep_score', 'mean'),
            has_data=('has_data', 'any')
        ).reset_index()

        # Filter valid sleep nights
        sleep_daily = sleep_daily[sleep_daily['total_duration'] >= 240].copy()
        return sleep_daily

    def normalize_hr(self, raw_df: pd.DataFrame):
        hr_df = raw_df[raw_df['Key'] == 'heart_rate'].copy()
        hr_df['Time'] = pd.to_datetime(hr_df['Time'], unit='s')

        hr_dicts = hr_df['Value'].apply(json.loads).tolist()
        hr_norm = pd.json_normalize(hr_dicts)

        hr_df = hr_df.reset_index(drop=True)
        hr_final = pd.concat([hr_df[['Time']], hr_norm], axis=1)

        hr_features = hr_final[['Time', 'avg_hr', 'min_hr', 'max_hr']].copy()
        hr_features['date_only'] = hr_features['Time'].dt.normalize()

        hr_daily = hr_features.groupby('date_only').agg(
            avg_hr=('avg_hr', 'mean'),
            min_hr=('min_hr', 'min'),
            max_hr=('max_hr', 'max'),
            has_data=('avg_hr', 'any')
        ).reset_index()
        return hr_daily

    def run(self):
        raw_df = self.load_raw_files()
        sleep_daily = self.normalize_sleep(raw_df)
        hr_daily = self.normalize_hr(raw_df)

        sleep_path = self.normalized_dir / "sleep_daily_normalized.csv"
        hr_path = self.normalized_dir / "hr_daily_normalized.csv"

        sleep_daily.to_csv(sleep_path, index=False)
        hr_daily.to_csv(hr_path, index=False)

        logger.info(f"Normalized sleep data saved: {sleep_path}, rows: {len(sleep_daily)}")
        logger.info(f"Normalized HR data saved: {hr_path}, rows: {len(hr_daily)}")

        return sleep_daily, hr_daily
