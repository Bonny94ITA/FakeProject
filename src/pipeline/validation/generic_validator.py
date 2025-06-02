import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Tuple

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, to_date, to_timestamp, when
from pyspark.sql.types import IntegerType, LongType


class GenericValidator:
    """Generic data validator using JSON-based validation rules."""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.validation_schemas_dir = Path(__file__).parent.parent.parent / "validation_schemas"

    def load_validation_schema(self, schema_name: str) -> Dict[str, Any]:
        """Load validation schema from JSON file."""
        schema_path = self.validation_schemas_dir / f"{schema_name}_validation.json"

        if not schema_path.exists():
            raise FileNotFoundError(f"Validation schema not found: {schema_path}")

        with open(schema_path, 'r') as f:
            schema = json.load(f)

        self.logger.info(f"Loaded validation schema: {schema_name}")
        return schema

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
            validation_rules = self.load_validation_schema(schema_name)

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
        """Apply all validations for a single column based on its attributes."""

        # 1. Required validation
        if config.get("required", False):
            df = df.withColumn(
                f"{column}_REQUIRED_VALID",
                when((col(column).isNotNull()) & (col(column) != ""), col(column)).otherwise(None)
            )
        else:
            # Optional fields always pass required check
            df = df.withColumn(f"{column}_REQUIRED_VALID", col(column))

        # 2. Regex validation
        if "regex" in config:
            df = df.withColumn(
                f"{column}_FORMAT_VALID",
                when(col(column).rlike(config["regex"]), col(column))
                .when(col(column).isNull() | (col(column) == ""), col(column))  # Allow nulls for optional fields
                .otherwise(None)
            )

        # 3. Allowed values validation
        if "allowed_values" in config:
            allowed_vals = config["allowed_values"]
            nullable = config.get("nullable", False)

            if nullable or None in allowed_vals or "" in allowed_vals:
                df = df.withColumn(
                    f"{column}_VALUES_VALID",
                    when(col(column).isin(allowed_vals), col(column))
                    .when(col(column).isNull() | (col(column) == ""), col(column))
                    .otherwise(None)
                )
            else:
                df = df.withColumn(
                    f"{column}_VALUES_VALID",
                    when(col(column).isin(allowed_vals), col(column)).otherwise(None)
                )

        # 4. Date format validation
        if config.get("type") == "date" and "date_format" in config:
            df = df.withColumn(
                f"{column}_DATE_VALID",
                to_date(col(column), config["date_format"])
            )

        # 5. Timestamp format validation
        if config.get("type") == "timestamp" and "timestamp_format" in config:
            df = df.withColumn(
                f"{column}_TIMESTAMP_VALID",
                to_timestamp(col(column), config["timestamp_format"])
            )

        # 6. Type casting (optional for validation phase)
        if "cast_to" in config:
            df = self._apply_type_casting(df, column, config["cast_to"], config.get("nullable", False))

        return df

    def _apply_type_casting(self, df: DataFrame, column: str, cast_type: str, nullable: bool) -> DataFrame:
        """Apply type casting for a column."""
        validation_col = f"{column}_CAST_VALID"

        if cast_type == "long":
            df = df.withColumn(validation_col, col(column).cast(LongType()))
        elif cast_type == "int":
            if nullable:
                df = df.withColumn(
                    validation_col,
                    when(col(column).isin("1", "2"), col(column).cast(IntegerType()))
                    .when(col(column).isNull() | (col(column) == ""), None)
                    .otherwise(None)
                )
            else:
                df = df.withColumn(validation_col, col(column).cast(IntegerType()))
        elif cast_type.startswith("decimal"):
            df = df.withColumn(validation_col, col(column).cast(cast_type))
        else:
            self.logger.warning(f"Unknown cast type: {cast_type}")

        return df

    def _split_valid_invalid(self, df: DataFrame, rules: Dict[str, Any]) -> Tuple[DataFrame, DataFrame]:
        """Split DataFrame into valid and invalid records based on column attributes."""

        # Build validation condition ONLY for required fields
        required_conditions = []
        base_columns = list(rules.keys())

        for column_name, column_config in rules.items():
            # Only check required fields
            if column_config.get("required", False):
                required_conditions.append(col(f"{column_name}_REQUIRED_VALID").isNotNull())

            # Add format validations if they exist AND field is required
            if "regex" in column_config and column_config.get("required", False):
                required_conditions.append(col(f"{column_name}_FORMAT_VALID").isNotNull())

            # Add value validations if they exist AND field is required AND not nullable
            if ("allowed_values" in column_config and
                column_config.get("required", False) and
                not column_config.get("nullable", False)):
                required_conditions.append(col(f"{column_name}_VALUES_VALID").isNotNull())

            # Add date validations if field is required
            if column_config.get("type") == "date" and column_config.get("required", False):
                required_conditions.append(col(f"{column_name}_DATE_VALID").isNotNull())

            # Add timestamp validations if field is required
            if column_config.get("type") == "timestamp" and column_config.get("required", False):
                required_conditions.append(col(f"{column_name}_TIMESTAMP_VALID").isNotNull())

        # Build overall validation condition
        if required_conditions:
            valid_condition = required_conditions[0]
            for condition in required_conditions[1:]:
                valid_condition = valid_condition & condition
        else:
            # If no required validations, all records are valid
            self.logger.info("No required validations found - all records considered valid")
            return df.select(*base_columns), df.limit(0).select(*base_columns)

        # Create clean DataFrames
        valid_df = self._create_clean_dataframe(df, base_columns, valid_condition, True)
        invalid_df = self._create_clean_dataframe(df, base_columns, valid_condition, False)
        invalid_df = self._add_error_messages(invalid_df, rules)

        return valid_df, invalid_df

    def _create_clean_dataframe(self, df: DataFrame, base_columns: List[str], valid_condition, is_valid: bool) -> DataFrame:
        """Create clean DataFrame with proper column selection."""
        if is_valid:
            clean_df = df.filter(valid_condition)
        else:
            clean_df = df.filter(~valid_condition)

        # Select only original columns
        select_columns = [col for col in df.columns if col in base_columns]
        return clean_df.select(*select_columns)

    def _add_error_messages(self, invalid_df: DataFrame, rules: Dict[str, Any]) -> DataFrame:
        """Add descriptive error messages based on column attributes."""
        error_condition = None

        for column_name, column_config in rules.items():
            # Skip optional fields for error messages
            if not column_config.get("required", False):
                continue

            description = column_config.get("description", f"Invalid {column_name}")
            role = column_config.get("role", "")

            if role == "primary_key":
                description = f"{description} (PK)"
            elif role == "foreign_key":
                description = f"{description} (FK)"

            if error_condition is None:
                error_condition = when(
                    col(column_name).isNull() | (col(column_name) == ""),
                    description
                )
            else:
                error_condition = error_condition.when(
                    col(column_name).isNull() | (col(column_name) == ""),
                    description
                )

        # Handle case where no required fields are invalid
        if error_condition is None:
            return invalid_df.withColumn("validation_error", lit("Unknown validation error"))

        return invalid_df.withColumn(
            "validation_error",
            error_condition.otherwise("Unknown validation error")
        )
