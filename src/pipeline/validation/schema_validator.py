import logging
from typing import Tuple

from pyspark.sql import DataFrame

from src.pipeline.validation.generic_validator import GenericValidator


class SchemaValidator:
    """Handles schema validation using generic JSON-based rules."""

    def __init__(self) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)
        self.generic_validator = GenericValidator()

    def validate_and_cast_contract(self, raw_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """Validate and cast contract data using JSON schema."""
        return self.generic_validator.validate_dataframe(raw_df, "contract")

    def validate_and_cast_claim(self, raw_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """Validate and cast claim data using JSON schema."""
        return self.generic_validator.validate_dataframe(raw_df, "claim")
