from ingestion.interfaces import DataSource
from config.settings import config
from config.logging import get_logger
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from pathlib import Path

logger = get_logger("GoogleSheetsSource")


class GoogleSheetsSource(DataSource):
    def __init__(self, sheet_name: str, csv_path: Path):
        self.sheet_name = sheet_name
        self.csv_path = csv_path
        self.gc = None
        self.ws = None

    def connect(self) -> None:
        """
        Authorize and connect to Google Sheets
        """
        logger.info(f"Connecting to Google Sheet: {self.sheet_name}")

        SCOPES = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.file",
            "https://www.googleapis.com/auth/drive"
        ]

        creds = Credentials.from_service_account_file(
            config.GOOGLE_SHEETS_KEY,
            scopes=SCOPES
        )
        self.gc = gspread.authorize(creds)

        sh = self.gc.open(self.sheet_name)
        self.ws = sh.sheet1

        logger.info("Connected successfully")

    def fetch(self) -> pd.DataFrame:
        """
        Fetch all records from the sheet and return as DataFrame
        """
        logger.info("Fetching data from Google Sheet")
        data = self.ws.get_all_records()
        df = pd.DataFrame(data)
        logger.info(f"Fetched {len(df)} rows")
        return df

    def normalize(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        """
        For now, we just pass through. Real normalization is done elsewhere.
        """
        return raw_data

    def store(self, normalized_data: pd.DataFrame) -> None:
        """
        Store raw snapshot of fetched Google Sheets data.
        Overwrites the single raw CSV.
        """
        if not self.csv_path.parent.exists():
            self.csv_path.parent.mkdir(parents=True, exist_ok=True)

        normalized_data.to_csv(self.csv_path, index=False)
        logger.info(f"Raw snapshot stored at {self.csv_path}, rows: {len(normalized_data)}")

