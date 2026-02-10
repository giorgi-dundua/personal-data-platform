import io
import re
import uuid
from pathlib import Path
from typing import List, Any
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from ingestion.interfaces import DataSource
from config.settings import config
from config.logging import get_logger

logger = get_logger("MiBandDriveSource")


class MiBandDriveSource(DataSource):
    """
    Ingest Mi Band CSV files from Google Drive using Atomic Rename Pattern.
    """

    def __init__(self):
        self.drive_folder_id = config.MI_BAND_DRIVE_FOLDER_ID
        self.raw_dir = config.RAW_MI_BAND_DATA_DIR
        self.service = None
        self._temp_paths: List[Path] = []

    def connect(self) -> None:
        """Authorize and connect to Google Drive API."""
        logger.info(f"Connecting to Google Drive folder: {self.drive_folder_id}")
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

    def fetch(self) -> List[Path]:
        """
        Download CSV files to unique temporary paths.
        Returns: List of temporary Paths.
        """
        logger.info(f"Fetching Mi Band files from Drive...")
        files = self._list_drive_csv_files()

        if not files:
            logger.warning("No Mi Band CSV files found.")
            return []

        self.raw_dir.mkdir(parents=True, exist_ok=True)

        downloaded_paths = []
        unique_id = uuid.uuid4().hex[:4]

        for file_info in files:
            file_name = file_info["name"]
            file_id = file_info["id"]

            # Generate unique temp path: MiFitness_data.csv.abcd.tmp
            clean_name = self._strip_timestamp(file_name)
            tmp_path = self.raw_dir / f"{clean_name}.{unique_id}.tmp"

            logger.info(f"Downloading: {file_name} -> {tmp_path.name}")

            request = self.service.files().get_media(fileId=file_id)
            with io.FileIO(tmp_path, "wb") as fh:
                downloader = MediaIoBaseDownload(fh, request, chunksize=8 * 1024 * 1024)
                done = False
                while not done:
                    _, done = downloader.next_chunk()

            downloaded_paths.append(tmp_path)

        # Store in instance for the store() method to retrieve
        self._temp_paths = downloaded_paths
        return downloaded_paths

    def normalize(self, raw_data: Any) -> Any:
        """Pass-through."""
        return raw_data

    def store(self, normalized_data: Any) -> List[Path]:
        """Returns the list of temporary paths for the Orchestrator to finalize."""
        return self._temp_paths

    # --- Helpers ---
    def _list_drive_csv_files(self):
        query = f"mimeType='text/csv' and '{self.drive_folder_id}' in parents"
        try:
            results = self.service.files().list(
                q=query, pageSize=1000, fields="files(id, name)"
            ).execute()
            return results.get("files", [])
        except Exception as e:
            logger.error(f"Error listing files: {e}")
            raise

    @staticmethod
    def _strip_timestamp(file_name: str) -> str:
        return re.sub(r"^\d+_\d+_", "", file_name)