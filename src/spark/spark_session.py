from typing import Optional

from pyspark.sql import SparkSession

from src.config import get_spark_config


def get_spark_session(app_name: Optional[str] = None) -> SparkSession:
    """
    Create Spark session using configuration.

    Args:
        app_name: Override default app name

    Returns:
        Configured SparkSession
    """
    spark_config = get_spark_config()

    app_name = app_name or spark_config["app_name"]
    master = spark_config["master"]

    return SparkSession.builder \
        .appName(app_name) \
        .master(master) \
        .getOrCreate()
