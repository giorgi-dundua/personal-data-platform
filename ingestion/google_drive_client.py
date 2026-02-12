import io
from pathlib import Path
from typing import Optional, List
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from google.oauth2.service_account import Credentials
from config.settings import config
from config.logging import get_logger

logger = get_logger("GoogleDriveClient")

class GoogleDriveClient:
    def __init__(self):
        self.service = None
        self._connect()

    def _connect(self):
        try:
            creds = Credentials.from_service_account_file(
                config.GOOGLE_SHEETS_KEY,
                scopes=config.GOOGLE_API_SCOPES
            )
            self.service = build("drive", "v3", credentials=creds)
        except Exception as e:
            logger.error(f"Failed to connect to Drive: {e}")
            raise

    def list_files(self, folder_id: str, mime_type: Optional[str] = None) -> List[dict]:
        query = f"'{folder_id}' in parents and trashed=false"
        if mime_type:
            query += f" and mimeType='{mime_type}'"
            
        results = self.service.files().list(
            q=query, fields="files(id, name)"
        ).execute()
        return results.get("files", [])

    def download_file(self, file_id: str, dest_path: Path):
        request = self.service.files().get_media(fileId=file_id)
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        
        with io.FileIO(dest_path, "wb") as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
        logger.info(f"Downloaded {dest_path.name}")

    def upload_file(self, local_path: Path, folder_id: str):
        if not local_path.exists():
            logger.warning(f"Cannot upload {local_path}: File not found")
            return

        file_metadata = {'name': local_path.name, 'parents': [folder_id]}
        media = MediaFileUpload(str(local_path), resumable=True)
        
        # Check if file exists to update it instead of creating duplicate
        existing = self.list_files(folder_id)
        target_id = next((f['id'] for f in existing if f['name'] == local_path.name), None)

        if target_id:
            # Update existing file
            self.service.files().update(
                fileId=target_id,
                media_body=media
            ).execute()
            logger.info(f"Updated remote file: {local_path.name}")
        else:
            # Create new file
            self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()
            logger.info(f"Uploaded new file: {local_path.name}")