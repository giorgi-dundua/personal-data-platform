import io
import re
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

from ingestion.interfaces import DataSource
from config.settings import config
from config.logging import get_logger

logger = get_logger("MiBandDriveSource")


class MiBandDriveSource(DataSource):
    """
    Ingest Mi Band CSV files from Google Drive.
    Downloads the latest files and saves canonical raw copies (no timestamp in filename).
    Normalization is handled separately in mi_band_normalizer.py.
    """

    def __init__(self):
        """
        Initialize using centralized config for Drive Folder ID and local storage paths.
        """
        self.drive_folder_id = config.MI_BAND_DRIVE_FOLDER_ID
        self.raw_dir = config.RAW_MI_BAND_DATA_DIR
        self.service = None

    def connect(self) -> None:
        """
        Authorize and connect to Google Drive API using scopes defined in AppConfig.
        """
        logger.info(f"Connecting to Google Drive for folder: {self.drive_folder_id}")

        try:
            creds = Credentials.from_service_account_file(
                config.GOOGLE_SHEETS_KEY,
                scopes=config.GOOGLE_API_SCOPES
            )
            self.service = build("drive", "v3", credentials=creds)
            logger.info("Connected to Google Drive successfully")
        except Exception as e:
            logger.error(f"Failed to connect to Google Drive: {e}")
            raise

    def fetch(self) -> None:
        """
        Download all CSV files from the Drive folder and save canonical raw copies.
        """
        logger.info(f"Fetching Mi Band CSV files from Drive folder: {self.drive_folder_id}")
        files = self._list_drive_csv_files()

        if not files:
            logger.warning("No Mi Band CSV files found in Drive folder")
            return

        self.raw_dir.mkdir(parents=True, exist_ok=True)

        for file_info in files:
            file_name = file_info["name"]
            file_id = file_info["id"]

            # Generate local path without the leading timestamp for easier processing
            raw_path = self.raw_dir / self._strip_timestamp(file_name)
            logger.info(f"Downloading file: {file_name} -> {raw_path}")

            request = self.service.files().get_media(fileId=file_id)

            # Using 8MB chunks for efficient transfer
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
        Placeholder to satisfy DataSource interface.
        Normalization logic resides in processing/normalizers/mi_band_normalizer.py.
        """
        return raw_data

    def store(self, normalized_data=None) -> None:
        """
        No-op. Raw files are already saved to disk during the fetch() process.
        """
        pass

    # ---- Helper methods ----

    def _list_drive_csv_files(self):
        """
        List all CSV files in the specified Drive folder using the Drive API.
        """
        query = f"mimeType='text/csv' and '{self.drive_folder_id}' in parents"
        try:
            results = self.service.files().list(
                q=query,
                pageSize=1000,
                fields="files(id, name)"
            ).execute()
            return results.get("files", [])
        except Exception as e:
            logger.error(f"Error listing files from Google Drive: {e}")
            return []

    @staticmethod
    def _strip_timestamp(file_name: str) -> str:
        """
        Remove leading timestamp patterns from filenames for deterministic naming.
        Example: '20260202_1593626543_MiFitness_data.csv' -> 'MiFitness_data.csv'
        """
        return re.sub(r"^\d+_\d+_", "", file_name)
