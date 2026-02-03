from typing import List
from ingestion.interfaces import DataSource
from config.logging import get_logger

logger = get_logger("pipeline")


class IngestionPipeline:
    def __init__(self, sources: List[DataSource]):
        self.sources = sources

    def run(self):
        logger.info("Ingestion pipeline started")

        for source in self.sources:
            source_name = source.__class__.__name__
            logger.info(f"Running source: {source_name}")

            try:
                source.connect()
                raw = source.fetch()
                normalized = source.normalize(raw)
                source.store(normalized)

                logger.info(f"Source completed: {source_name}")

            except Exception as e:
                logger.exception(f"Source failed: {source_name} | Error: {e}")

        logger.info("Ingestion pipeline finished")
