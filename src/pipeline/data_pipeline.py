import logging
from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession


class DataPipeline(ABC):
    """
    Template Method pattern for data pipelines.
    Defines the skeleton of the algorithm, delegating specific steps to subclasses.
    """

    def __init__(self, spark: SparkSession, input_dir: str, output_dir: str):
        self.spark = spark
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.logger = logging.getLogger(self.__class__.__name__)

    def execute(self) -> None:
        """
        Template method that defines the sequence of operations.
        """
        try:
            # 1. Extract
            input_data = self.extract()

            # 2. Validate
            valid_data, invalid_data = self.validate(input_data)

            # 3. Handle invalid data (delegated to concrete implementation)
            if invalid_data:
                self.handle_invalid_data(invalid_data)

            # 4. Transform
            transformed_data = self.transform(valid_data)

            # 5. Load
            self.load(transformed_data)

            self.logger.info("Pipeline completed successfully")

        except FileNotFoundError as e:
            self.logger.error(f"File not found error: {str(e)}")
            raise
        except ValueError as e:
            self.logger.error(f"Value error in pipeline: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error during pipeline execution: {str(e)}")
            raise

    @abstractmethod
    def extract(self) -> dict[str, DataFrame]:
        """Extract data from sources. To be implemented by subclasses."""
        pass

    @abstractmethod
    def validate(self, data: dict[str, DataFrame]) -> tuple[dict[str, DataFrame], dict[str, DataFrame]]:
        """
        Validate data and separate valid from invalid records.
        Returns a tuple (valid_data, invalid_data).
        """
        pass

    @abstractmethod
    def transform(self, data: dict[str, DataFrame]) -> DataFrame:
        """Transform data. To be implemented by subclasses."""
        pass

    @abstractmethod
    def load(self, data: DataFrame) -> None:
        """Load data into the destination. To be implemented by subclasses."""
        pass

    def handle_invalid_data(self, invalid_data: dict[str, DataFrame]) -> None:
        """
        Handle invalid data. Can be overridden by subclasses.
        Default implementation delegates to the loader.
        """
        if hasattr(self, 'loader'):
            self.loader.load_invalid_data(invalid_data)
        else:
            self.logger.warning("No loader configured for invalid data handling")
