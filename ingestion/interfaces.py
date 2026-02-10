from abc import ABC, abstractmethod
from typing import Any, List
from pathlib import Path


class DataSource(ABC):
    """
    Abstract interface for all ingestion sources.
    """

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the external source."""
        pass

    @abstractmethod
    def fetch(self) -> Any:
        """Fetch raw data from source."""
        pass

    @abstractmethod
    def normalize(self, raw_data: Any) -> Any:
        """Normalize raw data into canonical schema (optional)."""
        pass

    @abstractmethod
    def store(self, normalized_data: Any) -> List[Path]:
        """
        Store data to disk using atomic temporary files.

        Returns:
            List[Path]: A list of paths to the temporary files created.
        """
        pass