from ingestion.interfaces import DataSource
from config.settings import config
from config.logging import get_logger
from pathlib import Path
import io
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

logger = get_logger("MiBandDriveSource")


class MiBandDriveSource(DataSource):
    """
    Ingest Mi Band CSV files from Google Drive.
    Downloads the latest files and saves canonical raw copies (no timestamp in filename).
    Normalization is handled separately in mi_band_normalizer.py.
    """

    def __init__(self, drive_folder_id: str, raw_dir: Path):
        self.drive_folder_id = drive_folder_id
        self.raw_dir = raw_dir
        self.service = None

    def connect(self) -> None:
        """Authorize and connect to Google Drive API"""
        logger.info(f"Connecting to Google Drive for folder: {self.drive_folder_id}")

        SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
        creds = Credentials.from_service_account_file(config.GOOGLE_SHEETS_KEY, scopes=SCOPES)
        self.service = build("drive", "v3", credentials=creds)

        logger.info("Connected to Google Drive successfully")

    def fetch(self) -> None:
        """Download all CSV files from the Drive folder and save canonical raw copies"""
        logger.info(f"Fetching Mi Band CSV files from Drive folder: {self.drive_folder_id}")
        files = self._list_drive_csv_files()
        if not files:
            logger.warning("No Mi Band CSV files found in Drive folder")
            return

        self.raw_dir.mkdir(parents=True, exist_ok=True)

        for file_info in files:
            file_name = file_info["name"]
            file_id = file_info["id"]

            raw_path = self.raw_dir / self._strip_timestamp(file_name)
            logger.info(f"Downloading file: {file_name} -> {raw_path}")

            request = self.service.files().get_media(fileId=file_id)
            with io.FileIO(raw_path, "wb") as fh:
                downloader = MediaIoBaseDownload(fh, request, chunksize=8 * 1024 * 1024)
                done = False
                while not done:
                    status, done = downloader.next_chunk()
                    if status:
                        logger.debug(f"Download progress {file_name}: {int(status.progress() * 100)}%")

            logger.info(f"Downloaded and stored canonical raw file: {raw_path}")

    def normalize(self, raw_data=None):
        """
        Placeholder. Real Mi Band normalization is done in mi_band_normalizer.py.
        """
        return raw_data

    def store(self, normalized_data=None) -> None:
        """
        No-op for DataSource. Raw files are already saved in fetch().
        This method exists to satisfy the DataSource interface.
        """
        pass

    # ---- Helper methods ----

    def _list_drive_csv_files(self):
        """List all CSV files in the specified Drive folder"""
        query = f"mimeType='text/csv' and '{self.drive_folder_id}' in parents"
        results = self.service.files().list(
            q=query, pageSize=1000, fields="files(id, name)"
        ).execute()
        return results.get("files", [])

    @staticmethod
    def _strip_timestamp(file_name: str) -> str:
        """
        Remove leading timestamp (digits + underscore) from filename
        Example: '20260202_1593626543_MiFitness_data.csv' -> 'MiFitness_data.csv'
        """
        import re
        return re.sub(r"^\d+_\d+_", "", file_name)
