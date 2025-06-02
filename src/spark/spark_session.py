import logging
import os
from typing import Optional

from pyspark.sql import SparkSession

from src.config import get_spark_config

logger = logging.getLogger(__name__)


def get_spark_session(app_name: Optional[str] = None) -> SparkSession:
    """
    Create Spark session using configuration.

    Args:
        app_name: Override default app name

    Returns:
        Configured SparkSession
    """
    try:
        # Get Spark configuration
        spark_config = get_spark_config()

        # Get environment variables directly (instead of using get_config)
        app_name = os.getenv("SPARK_APP_NAME", "DataTransformer")
        master = os.getenv("SPARK_MASTER", "local[*]")

        # Create Spark session builder
        builder = SparkSession.builder \
            .appName(app_name) \
            .master(master)

        # Apply Spark configurations
        for key, value in spark_config.items():
            builder = builder.config(key, value)

        # Build session
        spark = builder.getOrCreate()

        # Set log level to reduce noise
        spark.sparkContext.setLogLevel("WARN")

        logger.info(f"Spark session created successfully: {app_name}")
        logger.info(f"Spark master: {master}")

        return spark

    except Exception as e:
        logger.error(f"Failed to create Spark session: {e}")
        raise
