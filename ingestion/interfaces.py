from abc import ABC, abstractmethod
from typing import Any

class DataSource(ABC):
    """
    Abstract interface for all ingestion sources.
    """

    @abstractmethod
    def connect(self) -> None:
        pass

    @abstractmethod
    def fetch(self) -> Any:
        """Fetch raw data from source."""
        pass

    @abstractmethod
    def normalize(self, raw_data: Any) -> Any:
        """Normalize raw data into canonical schema."""
        pass

    @abstractmethod
    def store(self, normalized_data: Any) -> None:
        """Store normalized or raw data snapshot."""
        pass
