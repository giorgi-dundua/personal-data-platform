from typing import List
from ingestion.interfaces import DataSource
from config.logging import get_logger

logger = get_logger("pipeline")


class IngestionPipeline:
    def __init__(self, sources: List[DataSource]):
        self.sources = sources

    def run(self):
        logger.info("Ingestion pipeline started")
        results = {"success": [], "failed": []}  # Track results

        for source in self.sources:
            source_name = source.__class__.__name__
            try:
                source.connect()
                raw = source.fetch()
                normalized = source.normalize(raw)
                source.store(normalized)
                results["success"].append(source_name)
            except Exception as e:
                logger.exception(f"Source failed: {source_name}")
                results["failed"].append(f"{source_name} ({type(e).__name__})")

        # Final summary
        logger.info(f"Ingestion finished. Success: {results['success']} | Failed: {results['failed']}")
