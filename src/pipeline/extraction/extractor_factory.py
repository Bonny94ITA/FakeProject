from pyspark.sql import SparkSession

from src.config import get_file_format_config
from src.pipeline.extraction.base_data_extractor import BaseDataExtractor
from src.pipeline.extraction.csv_extractor import CsvExtractor
from src.pipeline.schema.data_schemas import SchemaManager


class DataExtractorFactory:
    """Factory for creating data extractors with proper configuration."""

    @staticmethod
    def create_extractor(spark: SparkSession, data_type: str,
                        extractor_type: str = None) -> BaseDataExtractor:
        """
        Creates an extractor for the specified data type and format.

        Args:
            spark: SparkSession instance
            data_type: Type of data ('contract', 'claim', etc.)
            extractor_type: Type of extractor ('csv', 'json', 'parquet').
                           If None, uses config from INPUT_FILE_FORMAT

        Returns:
            BaseDataExtractor: Concrete implementation based on format
        """
        # Get extractor type from config if not specified
        if extractor_type is None:
            file_config = get_file_format_config()
            extractor_type = file_config["input_format"].lower()

        # Get schema using convention
        schema_method_name = f"get_{data_type}_raw_schema"
        if not hasattr(SchemaManager, schema_method_name):
            raise ValueError(f"No schema method found for data type '{data_type}'. Expected: {schema_method_name}")

        schema_method = getattr(SchemaManager, schema_method_name)
        schema = schema_method()

        # Factory logic - create extractor based on format
        if extractor_type == "csv":
            return CsvExtractor(spark, schema)
        elif extractor_type == "json":
            # Future: JsonExtractor(spark, schema)
            raise NotImplementedError("JSON extractor not yet implemented")
        elif extractor_type == "parquet":
            # Future: ParquetExtractor(spark, schema)
            raise NotImplementedError("Parquet extractor not yet implemented")
        elif extractor_type == "delta":
            # Future: DeltaExtractor(spark, schema)
            raise NotImplementedError("Delta extractor not yet implemented")
        else:
            supported_formats = ["csv", "json", "parquet", "delta"]
            raise ValueError(f"Unsupported extractor type '{extractor_type}'. Supported: {supported_formats}")
