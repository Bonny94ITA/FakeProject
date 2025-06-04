import json
import logging
from pathlib import Path
from typing import Any, Dict, Tuple

from pyspark.sql import DataFrame
from pyspark.sql.functions import array, col, concat_ws, lit, to_date, to_timestamp, when
from pyspark.sql.functions import col as spark_col
from pyspark.sql.types import IntegerType, LongType

from src.config import get_path_config


class SchemaValidator:
    """Generic data validator using JSON-based validation rules."""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        path_config = get_path_config()
        self.validation_schemas_dir = Path(path_config["validation_schemas_path"])

    def validate_dataframe(self, df: DataFrame, schema_name: str) -> Tuple[DataFrame, DataFrame]:
        """
        Validate DataFrame using the specified validation schema.

        Args:
            df: Input DataFrame to validate
            schema_name: Name of validation schema (e.g., 'contract', 'claim')

        Returns:
            Tuple of (valid_df, invalid_df)
        """
        try:
            validation_rules = self._load_validation_schema(schema_name, df)

            # Handle column name fixes first
            df = self._fix_column_names(df, schema_name)

            # Apply validation rules
            validated_df = self._apply_validation_rules(df, validation_rules)

            # Split into valid and invalid
            valid_df, invalid_df = self._split_valid_invalid(validated_df, validation_rules)

            # Log results
            valid_count = valid_df.count()
            invalid_count = invalid_df.count()

            self.logger.info(f"{schema_name.title()} validation: {valid_count} valid, {invalid_count} invalid")
            if invalid_count > 0:
                self.logger.warning(f"Invalid rate: {invalid_count}/{valid_count + invalid_count} records")

            return valid_df, invalid_df

        except Exception as e:
            self.logger.error(f"Error in {schema_name} validation: {e}")
            raise

    def _load_validation_schema(self, schema_name: str, df: DataFrame) -> Dict[str, Any]:
        """Load validation schema from JSON file and validate CSV structure."""

        schema_path = self.validation_schemas_dir / f"{schema_name}_validation.json"

        if not schema_path.exists():
            raise FileNotFoundError(f"Validation schema not found: {schema_path}")

        try:
            with open(schema_path, 'r') as f:
                schema = json.load(f)

            if not schema:
                raise ValueError(f"Empty schema file: {schema_path}")

            # Validate CSV columns against schema
            csv_columns = set(df.columns)
            schema_columns = set(schema.keys())

            # Check for malformed headers
            header_str = ','.join(df.columns)
            if '""' in header_str or any('"' in col for col in df.columns):
                raise ValueError(f"INVALID CSV FORMAT: Malformed headers detected in {schema_name} file")

            # Check each schema column exists in CSV (with typo variants)
            missing_cols = []
            for schema_col in schema_columns:
                # Define typo variants for known cases
                variants = [schema_col]
                if schema_col == "CONTRACT_ID":
                    variants.append("CONTRAT_ID")
                elif schema_col == "INSURED_PERIOD_TO":
                    variants.append("INSUDRED_PERIOD_TO")

                # Check if any variant exists
                if not any(variant in csv_columns for variant in variants):
                    missing_cols.append(schema_col)

            if missing_cols:
                raise ValueError(f"INVALID CSV FORMAT: Missing required columns in {schema_name} file: {missing_cols}")

            self.logger.info(f"Loaded validation schema: {schema_name}")
            return schema

        except json.JSONDecodeError:
            raise ValueError("INVALID CSV FORMAT: Malformed JSON in schema file")
        except Exception as e:
            if "INVALID CSV FORMAT" in str(e):
                raise  # Re-raise our custom errors
            raise IOError(f"Cannot read schema file: {e}")

    def _fix_column_names(self, df: DataFrame, schema_name: str) -> DataFrame:
        """Fix known column name issues."""
        if schema_name == "claim":
            # Fix typo: CONTRAT_ID -> CONTRACT_ID
            if "CONTRAT_ID" in df.columns:
                self.logger.info("Fixing column name: CONTRAT_ID -> CONTRACT_ID")
                df = df.withColumnRenamed("CONTRAT_ID", "CONTRACT_ID")
            elif "CONTRACT_ID" not in df.columns:
                raise ValueError("Neither 'CONTRACT_ID' nor 'CONTRAT_ID' found in claim data")

        elif schema_name == "contract":
            # Fix typo: INSUDRED_PERIOD_TO -> INSURED_PERIOD_TO
            if "INSUDRED_PERIOD_TO" in df.columns:
                self.logger.info("Fixing column name: INSUDRED_PERIOD_TO -> INSURED_PERIOD_TO")
                df = df.withColumnRenamed("INSUDRED_PERIOD_TO", "INSURED_PERIOD_TO")
            elif "INSURED_PERIOD_TO" not in df.columns:
                raise ValueError("Neither 'INSURED_PERIOD_TO' nor 'INSUDRED_PERIOD_TO' found in contract data")

        return df

    def _apply_validation_rules(self, df: DataFrame, rules: Dict[str, Any]) -> DataFrame:
        """Apply all validation rules to the DataFrame using column attributes."""
        validated_df = df

        for column_name, column_config in rules.items():
            if column_name not in df.columns:
                continue

            validated_df = self._apply_column_validation(validated_df, column_name, column_config)

        return validated_df

    def _apply_column_validation(self, df: DataFrame, column: str, config: Dict[str, Any]) -> DataFrame:
        """Apply validation for a single column with simplified logic."""

        validation_conditions = []

        # 1. Required validation
        if config.get("required", False):
            validation_conditions.append(
                (col(column).isNotNull()) & (col(column) != "")
            )

        # 2. Regex validation
        if "regex" in config:
            regex_condition = col(column).rlike(config["regex"])
            # Optional fields
            if not config.get("required", False):
                regex_condition = regex_condition | col(column).isNull() | (col(column) == "")
            validation_conditions.append(regex_condition)

        # 3. Allowed values validation
        if "allowed_values" in config:
            allowed_vals = config["allowed_values"]
            value_condition = col(column).isin(allowed_vals)
            # Optional fields
            if not config.get("required", False):
                value_condition = value_condition | col(column).isNull() | (col(column) == "")
            validation_conditions.append(value_condition)

        # 4. Date validation
        if config.get("type") == "date" and "date_format" in config:
            date_condition = to_date(col(column), config["date_format"]).isNotNull()
            # Optional fields
            if not config.get("required", False):
                date_condition = date_condition | col(column).isNull() | (col(column) == "")
            validation_conditions.append(date_condition)

        # 5. Timestamp validation
        if config.get("type") == "timestamp" and "timestamp_format" in config:
            ts_condition = to_timestamp(col(column), config["timestamp_format"]).isNotNull()
            # Optional fields
            if not config.get("required", False):
                ts_condition = ts_condition | col(column).isNull() | (col(column) == "")
            validation_conditions.append(ts_condition)

        if validation_conditions:
            final_condition = validation_conditions[0]
            for condition in validation_conditions[1:]:
                final_condition = final_condition & condition
            df = df.withColumn(f"{column}_IS_VALID", final_condition)
        else:
            df = df.withColumn(f"{column}_IS_VALID", lit(True))

        # Type casting if necessary
        if "cast_to" in config:
            df = self._apply_type_casting(df, column, config["cast_to"])

        return df

    def _apply_type_casting(self, df: DataFrame, column: str, cast_type: str) -> DataFrame:
        """Apply type casting for a column."""
        if cast_type == "long":
            df = df.withColumn(column, col(column).cast(LongType()))
        elif cast_type == "int":
            df = df.withColumn(column, col(column).cast(IntegerType()))
        elif cast_type.startswith("decimal"):
            df = df.withColumn(column, col(column).cast(cast_type))
        else:
            self.logger.warning(f"Unknown cast type: {cast_type}")

        return df

    def _split_valid_invalid(self, df: DataFrame, rules: Dict[str, Any]) -> Tuple[DataFrame, DataFrame]:
        """Split DataFrame into valid and invalid records based on validation columns."""

        validation_cols = [f"{col}_IS_VALID" for col in rules.keys() if f"{col}_IS_VALID" in df.columns]
        base_columns = list(rules.keys())

        if not validation_cols:
            # No validation columns found — treat all rows as valid
            return df.select(*base_columns), df.limit(0).select(*base_columns)

        # A record is valid only if all validations pass
        all_valid_condition = col(validation_cols[0])
        for val_col in validation_cols[1:]:
            all_valid_condition = all_valid_condition & col(val_col)

        # Filter valid records (only select base columns)
        valid_df = df.filter(all_valid_condition).select(*base_columns)

        # Keep all columns for invalid_df for error processing
        invalid_df = df.filter(~all_valid_condition)

        # Add error messages based on validation columns
        if invalid_df.count() > 0:
            invalid_df = self._add_error_messages(invalid_df, rules)
            # Now select base columns + validation_error (which now exists)
            final_invalid_df = invalid_df.select(*base_columns, "validation_error")
        else:
            # If there are no invalid records, create an empty DataFrame with validation_error
            final_invalid_df = invalid_df.select(*base_columns).withColumn("validation_error", lit(""))

        return valid_df, final_invalid_df


    def _add_error_messages(self, invalid_df: DataFrame, rules: Dict[str, Any]) -> DataFrame:
        """Add error messages showing which columns failed validation."""

        # Find all available validation columns
        validation_cols = [f"{col}_IS_VALID" for col in rules.keys() if f"{col}_IS_VALID" in invalid_df.columns]

        if not validation_cols:
            return invalid_df.withColumn("validation_error", lit("Validation failed"))

        failed_columns = []
        for val_col in validation_cols:
            column_name = val_col.replace("_IS_VALID", "")
            failed_columns.append(
                when(~spark_col(val_col), column_name).otherwise(None)
            )

        # Create an array of failed columns to generate a validation error message
        failed_array = array(*failed_columns)

        validation_error = concat_ws(", ", failed_array)

        return invalid_df.withColumn(
            "validation_error",
            when(validation_error != "", concat_ws("", lit("Failed columns: "), validation_error))
            .otherwise("Validation failed")
        )
