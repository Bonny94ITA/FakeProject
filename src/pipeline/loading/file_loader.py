from datetime import datetime
from typing import Any, Dict

from pyspark.sql import DataFrame

from src.config import join_paths
from src.pipeline.loading.base_loader import BaseLoader


class FileLoader(BaseLoader):
    """File-based loader supporting multiple formats (Parquet, JSON, CSV, etc.)."""

    def __init__(self, output_dir: str, config: Dict[str, Any]):
        super().__init__(output_dir, config)

        if "output_format" not in config:
            raise ValueError("output_format is required in config")

        self.output_format = config["output_format"].lower()

    def load(self, data: DataFrame, output_name: str = "TRANSACTIONS") -> None:
        """Load data in the configured output format with timestamp partitioning."""
        # Generate a timestamp for the partition
        batch_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = join_paths(self.output_dir, output_name, f"batch_date={batch_timestamp}")

        # Create directory if it doesn't exist
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Convert Path to string for PySpark compatibility
        output_path_str = str(output_path)

        self.logger.info(f"Saving {data.count()} records in {self.output_format.upper()} format to {output_path_str}")

        # Write data in the configured format
        self._write_data(data, output_path_str)

        self.logger.info(f"Data successfully written to {output_path_str}")

    def load_invalid_data(self, invalid_data: Dict[str, DataFrame]) -> None:
        """Load invalid data by writing it in the configured format."""
        if not invalid_data:
            self.logger.info("No invalid data to process")
            return

        # Count total invalid records first
        total_invalid = sum(df.count() for df in invalid_data.values())

        if total_invalid == 0:
            self.logger.info("No invalid records found - all data passed validation")
            return

        self.logger.warning("Processing invalid data for storage")
        self.logger.info(f"Invalid data keys: {list(invalid_data.keys())}")

        for name, df in invalid_data.items():
            record_count = df.count()
            if record_count > 0:
                invalid_path = join_paths(self.output_dir, "invalid", name)

                # Create directory if it doesn't exist
                invalid_path.parent.mkdir(parents=True, exist_ok=True)

                invalid_path_str = str(invalid_path)

                self.logger.info(f"Writing {record_count} invalid {name} records to {invalid_path_str} in {self.output_format.upper()} format")

                # Write invalid data
                self._write_data(df, invalid_path_str, mode="append")
            else:
                self.logger.debug(f"Skipping {name}: no invalid records")

    def _write_data(self, data: DataFrame, path: str, mode: str = "overwrite") -> None:
        """Internal method to write data in the specified format."""
        writer = data.write.mode(mode)

        if self.output_format == "parquet":
            writer.parquet(path)
        elif self.output_format == "json":
            writer.option("header", True).json(path)
        elif self.output_format == "csv":
            writer.option("header", True).csv(path)
        elif self.output_format == "delta":
            writer.format("delta").save(path)
        elif self.output_format == "orc":
            writer.orc(path)
        else:
            self.logger.warning(f"Unsupported output format '{self.output_format}'. Falling back to Parquet.")
            writer.parquet(path)
