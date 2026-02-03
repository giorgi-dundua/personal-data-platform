import pandas as pd
import logging
from config.settings import AppConfig
from processing.normalizers.mi_band_normalizer import MiBandNormalizer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger("MiBandPipeline")

# Device constants
DEVICE_START_DATE = pd.Timestamp("2023-11-22")
DEVICE_BUFFER_DAYS = 7
ANALYSIS_START_DATE = DEVICE_START_DATE + pd.Timedelta(days=DEVICE_BUFFER_DAYS)


class MiBandPipeline:
    """
    Full Mi Band normalization + validators pipeline.
    Saves normalized files to processed/normalized
    Saves validated files to processed/validated
    """

    def __init__(self):
        self.raw_dir = AppConfig.RAW_DATA_DIR / "mi_band"
        self.normalized_dir = AppConfig.PROCESSED_DATA_DIR / "normalized"
        self.validated_dir = AppConfig.PROCESSED_DATA_DIR / "validated"

        # Ensure directories exist
        self.normalized_dir.mkdir(parents=True, exist_ok=True)
        self.validated_dir.mkdir(parents=True, exist_ok=True)

        self.normalizer = MiBandNormalizer(self.raw_dir)

    def normalize(self):
        raw_df = self.normalizer.load_raw_files()
        sleep_daily = self.normalizer.normalize_sleep(raw_df)
        hr_daily = self.normalizer.normalize_hr(raw_df)

        # Derived column for visualization only
        sleep_daily['sleep_hours'] = sleep_daily['total_duration'] / 60

        # Reindex to continuous daily range
        sleep_index = pd.date_range(start=sleep_daily['date_only'].min(),
                                    end=sleep_daily['date_only'].max(),
                                    freq='D')
        hr_index = pd.date_range(start=hr_daily['date_only'].min(),
                                 end=hr_daily['date_only'].max(),
                                 freq='D')

        sleep_daily = sleep_daily.set_index('date_only').reindex(sleep_index).rename_axis('date_only').reset_index()
        hr_daily = hr_daily.set_index('date_only').reindex(hr_index).rename_axis('date_only').reset_index()

        # Apply device cutoff
        sleep_daily = sleep_daily[sleep_daily['date_only'] >= ANALYSIS_START_DATE].copy()
        hr_daily = hr_daily[hr_daily['date_only'] >= ANALYSIS_START_DATE].copy()

        # Save normalized CSVs
        sleep_daily.to_csv(self.normalized_dir / "sleep_daily_normalized.csv", index=False)
        hr_daily.to_csv(self.normalized_dir / "hr_daily_normalized.csv", index=False)

        logger.info(f"Normalized sleep saved: {self.normalized_dir / 'sleep_daily_normalized.csv'}, rows: {len(sleep_daily)}")
        logger.info(f"Normalized HR saved: {self.normalized_dir / 'hr_daily_normalized.csv'}, rows: {len(hr_daily)}")

        return sleep_daily, hr_daily

    def validate(self, sleep_daily: pd.DataFrame, hr_daily: pd.DataFrame):
        def validate_df(df: pd.DataFrame, name: str):
            logger.info(f"Validating {name}")
            row_count = len(df)
            duplicates = df.duplicated().sum()
            missing = df.isna().sum().to_dict()
            numeric_summary = {col: {"min": df[col].min(),
                                     "max": df[col].max(),
                                     "mean": df[col].mean()}
                               for col in df.select_dtypes(include="number")}
            logger.info(f"{name}: rows={row_count}, duplicates={duplicates}, missing={missing}, numeric_summary={numeric_summary}")
            return {
                "rows": row_count,
                "duplicates": duplicates,
                "missing": missing,
                "numeric_summary": numeric_summary
            }

        sleep_stats = validate_df(sleep_daily, "sleep_daily")
        hr_stats = validate_df(hr_daily, "hr_daily")

        # Save validated CSVs
        sleep_daily.to_csv(self.validated_dir / "sleep_daily_validated.csv", index=False)
        hr_daily.to_csv(self.validated_dir / "hr_daily_validated.csv", index=False)

        logger.info(f"Validated sleep saved: {self.validated_dir / 'sleep_daily_validated.csv'}")
        logger.info(f"Validated HR saved: {self.validated_dir / 'hr_daily_validated.csv'}")

        return sleep_stats, hr_stats

    def run(self):
        sleep_daily, hr_daily = self.normalize()
        sleep_stats, hr_stats = self.validate(sleep_daily, hr_daily)
        logger.info("Mi Band normalization + validators complete.")
        return sleep_stats, hr_stats


if __name__ == "__main__":
    pipeline = MiBandPipeline()
    pipeline.run()
