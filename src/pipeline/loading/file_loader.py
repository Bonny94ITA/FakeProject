from datetime import datetime
from typing import Any, Dict, Optional

from pyspark.sql import DataFrame

from src.config import get_file_format_config, get_format_write_options, join_paths
from src.pipeline.loading.base_loader import BaseLoader


class FileLoader(BaseLoader):
    """File-based loader supporting multiple formats (Parquet, JSON, CSV, etc.)."""

    def __init__(self, output_dir: str, config: Optional[Dict[str, Any]] = None):
        super().__init__(output_dir)
        # Fix: Use the config parameter or get default
        if config is None:
            config = get_file_format_config()

        self.config = config  # Fix: Assign to self.config instead of unused local variable

        if "output_format" not in self.config:
            raise ValueError("output_format is required in config")

        self.output_format = self.config["output_format"].lower()

    def load(self, data: DataFrame, output_name: str = "TRANSACTIONS") -> None:
        """Load data in the configured output format with timestamp partitioning."""
        # Generate a timestamp for the partition
        batch_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = join_paths(self.output_dir, output_name, f"batch_date={batch_timestamp}")

        # Create directory if it doesn't exist
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Convert Path to string for PySpark compatibility
        output_path_str = str(output_path)

        # Use the internal write method
        self._write_data(data, output_path_str)

        self.logger.info(f"Data loaded successfully to {output_path_str}")

    def load_invalid_data(self, invalid_data: Dict[str, DataFrame]) -> None:
        """Load invalid data for analysis."""
        try:
            invalid_base_path = join_paths(self.output_dir, "invalid")

            for data_type, df in invalid_data.items():
                if df.count() > 0:
                    invalid_path = join_paths(invalid_base_path, data_type)
                    invalid_path_str = str(invalid_path)

                    self.logger.info(f"Loading {df.count()} invalid {data_type} records to {invalid_path_str}")
                    self._write_data(df, invalid_path_str)
                    self.logger.info(f"Successfully loaded invalid {data_type} to {invalid_path_str}")
                else:
                    self.logger.info(f"No invalid {data_type} records to load")

        except Exception as e:
            self.logger.error(f"Error loading invalid data: {e}")
            raise

    def _write_data(self, data: DataFrame, path: str, mode: str = "overwrite") -> None:
        """Internal method to write data in the specified format."""
        writer = data.write.mode(mode)
        format_name = self.config["output_format"].lower()

        # Apply common options for the format
        format_options = get_format_write_options().get(format_name, {})
        for key, value in format_options.items():
            writer = writer.option(key, value)

        # Write in the appropriate format
        if format_name == "parquet":
            writer.parquet(path)
        elif format_name == "json":
            writer.json(path)
        elif format_name == "csv":
            writer.csv(path)
        elif format_name == "delta":
            writer.format("delta").save(path)
        elif format_name == "orc":
            writer.orc(path)
        else:
            self.logger.warning(f"Unsupported output format '{format_name}'. Falling back to Parquet.")
            writer.parquet(path)
