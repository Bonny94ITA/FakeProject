import logging
from typing import Tuple

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date, to_timestamp, when

from src.config import get_schema_config


class SchemaValidator:
    """Handles schema validation and type conversion for raw data."""

    def __init__(self) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)
        self.config = get_schema_config()

    def validate_and_cast_contract(self, raw_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """Validate and cast contract data from raw strings to proper types."""
        return self._validate_contract_data(raw_df)

    def validate_and_cast_claim(self, raw_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """Validate and cast claim data from raw strings to proper types."""
        return self._validate_claim_data(raw_df)

    def _validate_contract_data(self, raw_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """Internal method for contract validation logic."""
        try:
            validated_df = raw_df

            # Validate SOURCE_SYSTEM (PK) - must not be null or empty
            validated_df = validated_df.withColumn(
                "SOURCE_SYSTEM_VALID",
                when((col("SOURCE_SYSTEM").isNotNull()) &
                     (col("SOURCE_SYSTEM") != ""), col("SOURCE_SYSTEM")).otherwise(None)
            )

            # Validate CONTRACT_ID (PK) - must be numeric
            validated_df = validated_df.withColumn(
                "CONTRACT_ID_VALID",
                when(col("CONTRACT_ID").rlike("^[0-9]+$"),
                     col("CONTRACT_ID").cast("long")).otherwise(None)
            )

            # Parse date fields using dd.MM.yyyy format
            validated_df = validated_df.withColumn(
                "INSURED_PERIOD_FROM_VALID",
                to_date(col("INSURED_PERIOD_FROM"), "dd.MM.yyyy")
            ).withColumn(
                "INSURED_PERIOD_TO_VALID",
                to_date(col("INSURED_PERIOD_TO"), "dd.MM.yyyy")
            ).withColumn(
                "CREATION_DATE_VALID",
                to_timestamp(col("CREATION_DATE"), "dd.MM.yyyy HH:mm")
            )

            # Build validation condition for required fields
            valid_condition = (
                col("SOURCE_SYSTEM_VALID").isNotNull() &
                col("CONTRACT_ID_VALID").isNotNull() &
                col("INSURED_PERIOD_FROM_VALID").isNotNull() &
                col("INSURED_PERIOD_TO_VALID").isNotNull() &
                col("CREATION_DATE_VALID").isNotNull()
            )

            # Create valid records with clean column names
            valid_df = validated_df.filter(valid_condition).select(
                col("SOURCE_SYSTEM_VALID").alias("SOURCE_SYSTEM"),
                col("CONTRACT_ID_VALID").alias("CONTRACT_ID"),
                "CONTRACT_TYPE",  # Can be empty
                col("INSURED_PERIOD_FROM_VALID").alias("INSURED_PERIOD_FROM"),
                col("INSURED_PERIOD_TO_VALID").alias("INSURED_PERIOD_TO"),
                col("CREATION_DATE_VALID").alias("CREATION_DATE")
            )

            # Create invalid records with original data
            invalid_df = validated_df.filter(~valid_condition).select(
                "SOURCE_SYSTEM", "CONTRACT_ID", "CONTRACT_TYPE",
                "INSURED_PERIOD_FROM", "INSURED_PERIOD_TO", "CREATION_DATE"
            )

            # Add simple error descriptions
            invalid_df = invalid_df.withColumn("validation_error",
                when(col("SOURCE_SYSTEM").isNull() | (col("SOURCE_SYSTEM") == ""), "Invalid SOURCE_SYSTEM")
                .when(~col("CONTRACT_ID").rlike("^[0-9]+$"), "Invalid CONTRACT_ID")
                .when(to_date(col("INSURED_PERIOD_FROM"), "dd.MM.yyyy").isNull(), "Invalid INSURED_PERIOD_FROM")
                .when(to_date(col("INSURED_PERIOD_TO"), "dd.MM.yyyy").isNull(), "Invalid INSURED_PERIOD_TO")
                .when(to_timestamp(col("CREATION_DATE"), "dd.MM.yyyy HH:mm").isNull(), "Invalid CREATION_DATE")
                .otherwise("Unknown validation error")
            )

            # Log results
            valid_count = valid_df.count()
            invalid_count = invalid_df.count()

            self.logger.info(f"Contract validation: {valid_count} valid, {invalid_count} invalid")
            if invalid_count > 0:
                self.logger.warning(f"Invalid rate: {invalid_count}/{valid_count + invalid_count} records")

            return valid_df, invalid_df

        except Exception as e:
            self.logger.error(f"Error in contract validation: {e}")
            raise

    def _validate_claim_data(self, raw_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """Internal method for claim validation logic."""
        try:
            validated_df = raw_df

            # Fix typo in source data: CONTRAT_ID -> CONTRACT_ID
            if "CONTRAT_ID" in validated_df.columns:
                self.logger.info("Standardizing column name: CONTRAT_ID -> CONTRACT_ID")
                validated_df = validated_df.withColumnRenamed("CONTRAT_ID", "CONTRACT_ID")
            elif "CONTRACT_ID" not in validated_df.columns:
                raise ValueError("Neither 'CONTRACT_ID' nor 'CONTRAT_ID' found in claim data")

            # Validate SOURCE_SYSTEM (PK) - must not be null or empty
            validated_df = validated_df.withColumn(
                "SOURCE_SYSTEM_VALID",
                when((col("SOURCE_SYSTEM").isNotNull()) &
                     (col("SOURCE_SYSTEM") != ""), col("SOURCE_SYSTEM")).otherwise(None)
            )

            # Validate CLAIM_ID (PK) - handle string format with prefix
            validated_df = validated_df.withColumn(
                "CLAIM_ID_VALID",
                when(col("CLAIM_ID").rlike("^[A-Z]+_[0-9]+$"), col("CLAIM_ID"))
                .otherwise(None)
            )

            # Validate CONTRACT_SOURCE_SYSTEM (PK) - must not be null or empty
            validated_df = validated_df.withColumn(
                "CONTRACT_SOURCE_SYSTEM_VALID",
                when((col("CONTRACT_SOURCE_SYSTEM").isNotNull()) &
                     (col("CONTRACT_SOURCE_SYSTEM") != ""), col("CONTRACT_SOURCE_SYSTEM")).otherwise(None)
            )

            # Validate CONTRACT_ID (PK) - must be numeric
            validated_df = validated_df.withColumn(
                "CONTRACT_ID_VALID",
                when(col("CONTRACT_ID").rlike("^[0-9]+$"),
                     col("CONTRACT_ID").cast("long")).otherwise(None)
            )

            # CLAIM_TYPE should be 1, 2, or empty (empty is allowed)
            validated_df = validated_df.withColumn(
                "CLAIM_TYPE_VALID",
                when(col("CLAIM_TYPE").isin("1", "2"), col("CLAIM_TYPE").cast("int"))
                .when(col("CLAIM_TYPE").isNull() | (col("CLAIM_TYPE") == ""), None)
                .otherwise(None)  # Invalid value
            )

            # AMOUNT must be a valid decimal number
            validated_df = validated_df.withColumn(
                "AMOUNT_VALID",
                when(col("AMOUNT").rlike("^[0-9]+\\.?[0-9]*$"),
                     col("AMOUNT").cast("decimal(16,5)")).otherwise(None)
            )

            # Parse date fields
            validated_df = validated_df.withColumn(
                "DATE_OF_LOSS_VALID",
                to_date(col("DATE_OF_LOSS"), "dd.MM.yyyy")
            ).withColumn(
                "CREATION_DATE_VALID",
                to_timestamp(col("CREATION_DATE"), "dd.MM.yyyy HH:mm")
            )

            # Build validation condition for required fields
            valid_condition = (
                col("SOURCE_SYSTEM_VALID").isNotNull() &
                col("CLAIM_ID_VALID").isNotNull() &
                col("CONTRACT_SOURCE_SYSTEM_VALID").isNotNull() &
                col("CONTRACT_ID_VALID").isNotNull() &
                col("AMOUNT_VALID").isNotNull() &
                col("DATE_OF_LOSS_VALID").isNotNull() &
                col("CREATION_DATE_VALID").isNotNull()
            )

            # Create valid records with clean column names
            valid_df = validated_df.filter(valid_condition).select(
                col("SOURCE_SYSTEM_VALID").alias("SOURCE_SYSTEM"),
                col("CLAIM_ID_VALID").alias("CLAIM_ID"),
                col("CONTRACT_SOURCE_SYSTEM_VALID").alias("CONTRACT_SOURCE_SYSTEM"),
                col("CONTRACT_ID_VALID").alias("CONTRACT_ID"),
                col("CLAIM_TYPE_VALID").alias("CLAIM_TYPE"),
                col("DATE_OF_LOSS_VALID").alias("DATE_OF_LOSS"),
                col("AMOUNT_VALID").alias("AMOUNT"),
                col("CREATION_DATE_VALID").alias("CREATION_DATE")
            )

            # Create invalid records with original data
            invalid_df = validated_df.filter(~valid_condition).select(
                "SOURCE_SYSTEM", "CLAIM_ID", "CONTRACT_SOURCE_SYSTEM", "CONTRACT_ID",
                "CLAIM_TYPE", "DATE_OF_LOSS", "AMOUNT", "CREATION_DATE"
            )

            # Add simple error descriptions
            invalid_df = invalid_df.withColumn("validation_error",
                when(col("SOURCE_SYSTEM").isNull() | (col("SOURCE_SYSTEM") == ""), "Invalid SOURCE_SYSTEM")
                .when(~col("CLAIM_ID").rlike("^[A-Z]+_[0-9]+$"), "Invalid CLAIM_ID")
                .when(col("CONTRACT_SOURCE_SYSTEM").isNull() | (col("CONTRACT_SOURCE_SYSTEM") == ""), "Invalid CONTRACT_SOURCE_SYSTEM")
                .when(~col("CONTRACT_ID").rlike("^[0-9]+$"), "Invalid CONTRACT_ID")
                .when(~col("AMOUNT").rlike("^[0-9]+\\.?[0-9]*$"), "Invalid AMOUNT")
                .when(to_date(col("DATE_OF_LOSS"), "dd.MM.yyyy").isNull(), "Invalid DATE_OF_LOSS")
                .when(to_timestamp(col("CREATION_DATE"), "dd.MM.yyyy HH:mm").isNull(), "Invalid CREATION_DATE")
                .otherwise("Unknown validation error")
            )

            # Log results
            valid_count = valid_df.count()
            invalid_count = invalid_df.count()

            self.logger.info(f"Claim validation: {valid_count} valid, {invalid_count} invalid")
            if invalid_count > 0:
                self.logger.warning(f"Invalid rate: {invalid_count}/{valid_count + invalid_count} records")

            return valid_df, invalid_df

        except Exception as e:
            self.logger.error(f"Error in claim validation: {e}")
            raise
