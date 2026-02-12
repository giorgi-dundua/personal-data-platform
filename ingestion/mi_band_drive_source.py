import uuid
import re
from pathlib import Path
from typing import List, Any

from ingestion.interfaces import DataSource
from ingestion.google_drive_client import GoogleDriveClient
from config.settings import config
from config.logging import get_logger

logger = get_logger("MiBandDriveSource")


class MiBandDriveSource(DataSource):
    """
    Ingest Mi Band CSV files from Google Drive using the shared GoogleDriveClient.
    """

    def __init__(self):
        self.drive_folder_id = config.MI_BAND_DRIVE_FOLDER_ID
        self.raw_dir = config.RAW_MI_BAND_DATA_DIR
        self.client: GoogleDriveClient = None
        self._temp_paths: List[Path] = []

    def connect(self) -> None:
        """Initialize the shared Drive client."""
        # The client handles authentication internally
        self.client = GoogleDriveClient()

    def fetch(self) -> List[Path]:
        """
        Download CSV files to unique temporary paths.
        Returns: List of temporary Paths.
        """
        logger.info(f"Fetching Mi Band files from Drive...")
        
        # Use the client to list files
        files = self.client.list_files(self.drive_folder_id, mime_type='text/csv')
        
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
            
            # Delegate download to the client
            self.client.download_file(file_id, tmp_path)
            
            downloaded_paths.append(tmp_path)

        self._temp_paths = downloaded_paths
        return downloaded_paths

    def normalize(self, raw_data: Any) -> Any:
        """Pass-through."""
        return raw_data

    def store(self, normalized_data: Any) -> List[Path]:
        """Returns the list of temporary paths for the Orchestrator to finalize."""
        return self._temp_paths

    @staticmethod
    def _strip_timestamp(file_name: str) -> str:
        return re.sub(r"^\d+_\d+_", "", file_name)