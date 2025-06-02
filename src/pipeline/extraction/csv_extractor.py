
from pyspark.sql import DataFrame, SparkSession

from src.config import get_csv_options
from src.pipeline.extraction.base_data_extractor import BaseDataExtractor


class CsvExtractor(BaseDataExtractor):
    """CSV-specific implementation of data extractor."""

    def __init__(self, spark: SparkSession):
        super().__init__(spark)
        self.csv_options = get_csv_options()

    def extract(self, source_path: str) -> DataFrame:
        """Extract data from CSV file."""
        try:
            self.logger.info(f"Extracting from {source_path}")

            # Build reader with options
            reader = self.spark.read

            # Apply CSV options
            for option, value in self.csv_options.items():
                reader = reader.option(option, value)

            df = reader.csv(source_path)

            row_count = df.count()
            self.logger.info(f"Extracted {row_count} records")

            return df

        except Exception as e:
            self.logger.error(f"Error extracting from {source_path}: {e}")
            raise
