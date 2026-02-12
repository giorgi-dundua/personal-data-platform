"""
State Synchronization Script.
Pulls/Pushes the Registry and Merged Data to/from Google Drive.
Used to maintain state across stateless execution environments (GitHub Actions, Docker).
"""
import argparse
import sys
from pathlib import Path
from config.settings import config
from config.logging import setup_logging, get_logger
from ingestion.google_drive_client import GoogleDriveClient

setup_logging()
logger = get_logger("sync_state")

# Files to sync
# We sync the Registry (Metadata) and the Merged Output (The final dataset)
SYNC_TARGETS = [
    config.PROCESSED_DATA_DIR / "registry.db",
    config.merged_path
]

def pull_state(client: GoogleDriveClient, folder_id: str):
    """Download state files from Drive if they exist."""
    logger.info("🔻 Pulling state from Google Drive...")
    
    remote_files = client.list_files(folder_id)
    remote_map = {f['name']: f['id'] for f in remote_files}

    for local_path in SYNC_TARGETS:
        if local_path.name in remote_map:
            logger.info(f"Found remote {local_path.name}, downloading...")
            client.download_file(remote_map[local_path.name], local_path)
        else:
            logger.warning(f"Remote {local_path.name} not found. Starting fresh.")

def push_state(client: GoogleDriveClient, folder_id: str):
    """Upload local state files to Drive."""
    logger.info("🔺 Pushing state to Google Drive...")

    for local_path in SYNC_TARGETS:
        if local_path.exists():
            logger.info(f"Uploading {local_path.name}...")
            client.upload_file(local_path, folder_id)
        else:
            logger.warning(f"Local {local_path.name} missing. Skipping upload.")

def main():
    parser = argparse.ArgumentParser(description="Sync Pipeline State with Google Drive")
    parser.add_argument("action", choices=["pull", "push"], help="Action to perform")
    args = parser.parse_args()

    folder_id = config.GOOGLE_DRIVE_STATE_FOLDER_ID
    if not folder_id:
        logger.critical("GOOGLE_DRIVE_STATE_FOLDER_ID is not set in config.")
        sys.exit(1)

    try:
        client = GoogleDriveClient()
        if args.action == "pull":
            pull_state(client, folder_id)
        elif args.action == "push":
            push_state(client, folder_id)
    except Exception as e:
        logger.critical(f"Sync failed: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()