import pandas as pd
from config.logging import get_logger

logger = get_logger("Validator")


class Validator:
    """
    Generic validator for any row-level CSV dataset.
    """

    def __init__(self, input_path, output_path, required_columns: list[str], date_col: str):
        self.input_path = input_path
        self.output_path = output_path
        self.required_columns = required_columns
        self.date_col = date_col

    def load_data(self) -> pd.DataFrame:
        if not self.input_path.exists():
            raise FileNotFoundError(f"Input file not found: {self.input_path}")

        logger.info(f"Loading data: {self.input_path.name}")
        return pd.read_csv(self.input_path, parse_dates=[self.date_col])

    def run(self) -> pd.DataFrame:
        df = self.load_data()

        # 1. Ensure required columns exist (create empty if missing)
        for col in self.required_columns:
            if col not in df.columns:
                logger.warning(f"Missing column '{col}' - creating as NA")
                df[col] = pd.NA

        # 2. Cleanup
        before = len(df)
        # Drop rows where critical columns are null
        df = df.dropna(subset=list(self.required_columns))
        # Drop exact duplicates
        df = df.drop_duplicates(subset=list(self.required_columns))

        logger.info(f"Validation cleaned {before - len(df)} rows")

        # 3. Save
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(self.output_path, index=False)

        logger.info(f"Validated data saved to {self.output_path}")
        return df

    def run_with_count(self):
        df = self.run()
        return df, len(df)
