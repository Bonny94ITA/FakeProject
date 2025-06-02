import logging
from abc import ABC, abstractmethod
from typing import Dict

from pyspark.sql import DataFrame


class BaseLoader(ABC):
    """Base abstract class for all data loaders."""

    def __init__(self, output_dir: str):
        self.output_dir = output_dir
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def load(self, data: DataFrame, output_name: str = "data") -> None:
        """Load data to the destination."""
        pass

    @abstractmethod
    def load_invalid_data(self, invalid_data: Dict[str, DataFrame]) -> None:
        """Load invalid data for analysis."""
        pass
