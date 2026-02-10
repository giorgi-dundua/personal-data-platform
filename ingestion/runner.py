"""
Ingestion Runner.
Coordinates multiple DataSources and aggregates their output paths.
Implements 'Strict Mode': If any source fails, the entire stage is rolled back.
"""
from typing import List
from pathlib import Path
from ingestion.interfaces import DataSource
from config.logging import get_logger

logger = get_logger("ingestion_runner")


class IngestionRunner:
    """
    Executes a list of DataSources and collects the paths of all created artifacts.
    """

    def __init__(self, sources: List[DataSource]):
        self.sources = sources

    def run(self) -> List[Path]:
        """
        Runs the ingestion lifecycle for all configured sources.

        Returns:
            List[Path]: A combined list of all temporary file paths created.

        Raises:
            RuntimeError: If ANY source fails, all temporary files are deleted
                          and the error is re-raised.
        """
        logger.info("Ingestion pipeline started (Strict Mode)")
        all_temp_paths: List[Path] = []

        # Initialize variable so it exists even if the loop crashes immediately
        source_name = "Unknown Source"

        try:
            for source in self.sources:
                source_name = source.__class__.__name__

                # 1. Connection & Retrieval
                source.connect()
                raw_data = source.fetch()

                # 2. Normalization (Source-specific)
                normalized = source.normalize(raw_data)

                # 3. Storage (Returns List of Paths)
                new_paths = source.store(normalized)

                if new_paths:
                    all_temp_paths.extend(new_paths)
                    logger.info(f"Source '{source_name}' produced {len(new_paths)} artifacts.")

        except Exception as e:
            logger.error(f"Ingestion failed at source: {source_name} | Error: {e}")

            # --- ROLLBACK TRANSACTION ---
            logger.warning("Rolling back: Cleaning up temporary files...")
            for p in all_temp_paths:
                if p.exists():
                    try:
                        p.unlink()
                        logger.debug(f"Deleted orphan file: {p.name}")
                    except OSError as cleanup_err:
                        logger.warning(f"Failed to delete {p.name}: {cleanup_err}")

            raise RuntimeError(f"Ingestion Stage Failed. Rolled back {len(all_temp_paths)} files.") from e

        logger.info(f"Ingestion finished. Total artifacts collected: {len(all_temp_paths)}")
        return all_temp_paths