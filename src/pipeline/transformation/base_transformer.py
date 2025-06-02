import logging
from abc import ABC, abstractmethod
from typing import Dict

from pyspark.sql import DataFrame, SparkSession


class BaseTransformer(ABC):
    """Base abstract class for all data transformers."""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def transform(self, data: Dict[str, DataFrame]) -> DataFrame:
        """Transform data according to business rules."""
        pass
