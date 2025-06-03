import logging
import sys
from pathlib import Path

from src.config import get_path_config
from src.pipeline.contract_pipeline import ContractClaimPipeline
from src.spark.spark_session import get_spark_session

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Main function that runs the transformation pipeline."""
    spark = None  # Initialize to None to avoid UnboundLocalError
    try:
        logger.info("Starting data transformation")
        spark = get_spark_session()

        # Load configuration from .env
        path_config = get_path_config()
        input_dir = path_config["input_path"]
        output_dir = path_config["output_path"]

        # Ensure output directory exists
        Path(output_dir).mkdir(parents=True, exist_ok=True)

        # Create and execute the pipeline
        pipeline = ContractClaimPipeline(spark, input_dir, output_dir)
        pipeline.execute()

        logger.info("Transformation completed successfully")
    except Exception as e:
        logger.error(f"Error in execution: {str(e)}")
        sys.exit(1)
    finally:
        if spark is not None:  # Only stop if spark was successfully created
            try:
                spark.stop()
                logger.info("Spark session stopped successfully")
            except Exception as e:
                logger.error(f"Error stopping Spark: {e}")


if __name__ == "__main__":
    main()
