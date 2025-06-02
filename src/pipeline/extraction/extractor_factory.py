from pyspark.sql import SparkSession

from src.config import get_file_format_config
from src.pipeline.extraction.base_data_extractor import BaseDataExtractor
from src.pipeline.extraction.csv_extractor import CsvExtractor


class DataExtractorFactory:
    """Factory for creating data extractors with simplified configuration."""

    @staticmethod
    def create_extractor(spark: SparkSession) -> BaseDataExtractor:
        """
        Creates an extractor based on format type.

        Args:
            spark: SparkSession instance

        Returns:
            BaseDataExtractor: Concrete implementation based on format
        """
        # Get extractor type from config if not specified
        file_config = get_file_format_config()
        extractor_type = file_config["input_format"].lower()

        # Factory logic - create extractor based on format
        if extractor_type == "csv":
            return CsvExtractor(spark)
        elif extractor_type == "json":
            # Future: JsonExtractor(spark)
            raise NotImplementedError("JSON extractor not yet implemented")
        elif extractor_type == "parquet":
            # Future: ParquetExtractor(spark)
            raise NotImplementedError("Parquet extractor not yet implemented")
        elif extractor_type == "delta":
            # Future: DeltaExtractor(spark)
            raise NotImplementedError("Delta extractor not yet implemented")
        else:
            supported_formats = ["csv", "json", "parquet", "delta"]
            raise ValueError(f"Unsupported extractor type '{extractor_type}'. Supported: {supported_formats}")
