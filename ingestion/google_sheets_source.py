import uuid
import pandas as pd
import gspread
from pathlib import Path
from typing import List
from google.oauth2.service_account import Credentials

from ingestion.interfaces import DataSource
from config.settings import config
from config.logging import get_logger

logger = get_logger("GoogleSheetsSource")


class GoogleSheetsSource(DataSource):
    def __init__(self, sheet_name: str = None):
        """
        Initialize with sheet name. Defaults to config.GOOGLE_SHEET_NAME.
        """
        self.sheet_name = sheet_name or config.GOOGLE_SHEET_NAME
        self.csv_path = config.raw_gs_path
        self.gc = None
        self.ws = None

    def connect(self) -> None:
        """Authorize and connect to Google Sheets."""
        logger.info(f"Connecting to Google Sheet: {self.sheet_name}")
        try:
            creds = Credentials.from_service_account_file(
                config.GOOGLE_SHEETS_KEY,
                scopes=config.GOOGLE_API_SCOPES
            )
            self.gc = gspread.authorize(creds)
            sh = self.gc.open(self.sheet_name)
            self.ws = sh.sheet1
            logger.info("Connected successfully")
        except Exception as e:
            logger.error(f"Failed to connect to Google Sheets: {e}")
            raise

    def fetch(self) -> pd.DataFrame:
        """Fetch all records from the sheet."""
        logger.info(f"Fetching data from {self.sheet_name}")
        data = self.ws.get_all_records()
        df = pd.DataFrame(data)
        logger.info(f"Fetched {len(df)} rows")
        return df

    def normalize(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        """Pass-through (normalization happens in processing stage)."""
        return raw_data

    def store(self, normalized_data: pd.DataFrame) -> List[Path]:
        """
        Store raw snapshot to a unique temporary file.
        """
        self.csv_path.parent.mkdir(parents=True, exist_ok=True)

        # Atomic Write Pattern
        unique_id = uuid.uuid4().hex[:4]
        tmp_path = self.csv_path.with_suffix(f".csv.{unique_id}.tmp")

        normalized_data.to_csv(tmp_path, index=False)
        logger.info(f"Raw snapshot stored at {tmp_path}")

        return [tmp_path]