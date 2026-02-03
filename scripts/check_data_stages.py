import pandas as pd
from config.settings import AppConfig


class PipelineDataChecker:
    EXPECTED_COLUMNS = ['date', 'time', 'systolic', 'diastolic', 'pulse', 'context notes', 'dose mg']

    def __init__(self):
        self.raw_path = AppConfig.RAW_DATA_DIR / "google_sheets" / "bp_hr_google_sheets.csv"
        self.normalized_path = AppConfig.NORMALIZED_DATA_DIR / "bp_hr_normalized.csv"
        self.validated_path = AppConfig.PROCESSED_DATA_DIR / "validated_bp_hr.csv"
        self.stages = ['raw', 'normalized', 'validated']
        self.paths = {
            'raw': self.raw_path,
            'normalized': self.normalized_path,
            'validated': self.validated_path
        }

    @staticmethod
    def clean_columns(df):
        df = df.copy()
        df.columns = [str(c).strip().lower() for c in df.columns]
        return df

    @staticmethod
    def numeric_summary(df, numeric_cols):
        return df[numeric_cols].agg(['min', 'max', 'mean']).to_dict()

    def check_stage(self, stage_name, df):
        print(f"\n--- Checking stage: {stage_name} ---")

        # Standardize column names
        df = self.clean_columns(df)

        # Column check
        missing_cols = set(self.EXPECTED_COLUMNS) - set(df.columns)
        if missing_cols:
            print(f"Missing columns: {missing_cols}")
        else:
            print(f"All expected columns present.")

        # Row count
        print(f"Row count: {len(df)}")

        # Duplicates
        dup_count = df.duplicated().sum()
        print(f"Duplicate rows: {dup_count}")

        # Missing values
        missing_vals = df.isna().sum().to_dict()
        print(f"Missing values per column: {missing_vals}")

        # Numeric summary
        numeric_cols = ['systolic', 'diastolic', 'pulse', 'dose mg']
        numeric_cols_present = [c for c in numeric_cols if c in df.columns]
        if numeric_cols_present:
            summary = self.numeric_summary(df, numeric_cols_present)
            print(f"Numeric summary (min/max/mean): {summary}")
        else:
            print(f"No numeric columns found for summary.")

        return df

    def compare_stages(self, df1, df2, name1, name2):
        print(f"\n--- Comparing {name1} → {name2} ---")
        print(f"Row count change: {len(df1)} → {len(df2)}")

        numeric_cols = ['systolic', 'diastolic', 'pulse', 'dose mg']
        numeric_cols_present = [c for c in numeric_cols if c in df1.columns and c in df2.columns]
        for c in numeric_cols_present:
            diff_min = df2[c].min() - df1[c].min()
            diff_max = df2[c].max() - df1[c].max()
            diff_mean = df2[c].mean() - df1[c].mean()
            print(f"{c}: Δmin={diff_min}, Δmax={diff_max}, Δmean={diff_mean}")

    def run(self):
        dfs = {}
        # Load and check each stage
        for stage in self.stages:
            try:
                df = pd.read_csv(self.paths[stage])
                df_clean = self.check_stage(stage, df)
                dfs[stage] = df_clean
            except FileNotFoundError:
                print(f"File not found for stage '{stage}': {self.paths[stage]}")
                return

        # Compare stages
        self.compare_stages(dfs['raw'], dfs['normalized'], 'raw', 'normalized')
        self.compare_stages(dfs['normalized'], dfs['validated'], 'normalized', 'validated')


if __name__ == '__main__':
    checker = PipelineDataChecker()
    checker.run()
