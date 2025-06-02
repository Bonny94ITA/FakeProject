import logging
from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession


class BaseDataExtractor(ABC):
    """Base abstract class for all data extractors."""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def extract(self, source_path: str) -> DataFrame:
        """Extract data from the source."""
        pass
