from ingestion.interfaces import DataSource
from config.settings import config
from config.logging import get_logger
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd

logger = get_logger("GoogleSheetsSource")


class GoogleSheetsSource(DataSource):
    def __init__(self, sheet_name: str = "ADHD BP & HR"):
        """
        Initialize with sheet name. Paths are pulled from central config.
        """
        self.sheet_name = sheet_name
        self.csv_path = config.raw_gs_path
        self.gc = None
        self.ws = None

    def connect(self) -> None:
        """
        Authorize and connect to Google Sheets using credentials and scopes from config.
        """
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
        """
        Fetch all records from the sheet and return as DataFrame
        """
        logger.info(f"Fetching data from {self.sheet_name}")
        data = self.ws.get_all_records()
        df = pd.DataFrame(data)
        logger.info(f"Fetched {len(df)} rows")
        return df

    def normalize(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        """
        Pass-through. Real normalization is handled by GoogleSheetsNormalizer.
        """
        return raw_data

    def store(self, normalized_data: pd.DataFrame) -> None:
        """
        Store raw snapshot of fetched Google Sheets data.
        """
        self.csv_path.parent.mkdir(parents=True, exist_ok=True)
        normalized_data.to_csv(self.csv_path, index=False)
        logger.info(f"Raw snapshot stored at {self.csv_path}")
