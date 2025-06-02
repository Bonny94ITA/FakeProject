"""Data quality tests focused on real-world issues."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from src.pipeline.validation.schema_validator import SchemaValidator


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[*]").appName("DataQualityTest").getOrCreate()


def test_handles_typo_in_column_name(spark):
    """Test that CONTRAT_ID typo is handled correctly."""
    validator = SchemaValidator()
    
    # Create claim data with the real typo from CSV
    claim_schema = StructType([
        StructField("SOURCE_SYSTEM", StringType(), False),
        StructField("CLAIM_ID", StringType(), False),
        StructField("CONTRACT_SOURCE_SYSTEM", StringType(), False),
        StructField("CONTRAT_ID", StringType(), False),  # The typo!
        StructField("CLAIM_TYPE", StringType(), True),
        StructField("DATE_OF_LOSS", StringType(), True),
        StructField("AMOUNT", StringType(), True),
        StructField("CREATION_DATE", StringType(), True)
    ])
    
    test_data = [("Claim_SR_Europa_3", "CL_12345", "Contract_SR_Europa_3", "12345", "2", "01.01.2022", "1000.00", "02.01.2022 10:15")]
    claim_df = spark.createDataFrame(test_data, schema=claim_schema)
    
    valid_df, invalid_df = validator.validate_and_cast_claim(claim_df)
    
    # Should handle typo gracefully
    assert valid_df.count() == 1
    assert "CONTRACT_ID" in valid_df.columns  # Renamed correctly
    assert "CONTRAT_ID" not in valid_df.columns  # Typo removed


def test_primary_key_validation(spark):
    """Test that primary key violations are caught."""
    validator = SchemaValidator()
    
    contract_schema = StructType([
        StructField("SOURCE_SYSTEM", StringType(), False),
        StructField("CONTRACT_ID", StringType(), False),
        StructField("CONTRACT_TYPE", StringType(), True),
        StructField("INSURED_PERIOD_FROM", StringType(), True),
        StructField("INSURED_PERIOD_TO", StringType(), True),
        StructField("CREATION_DATE", StringType(), True)
    ])
    
    # Mix of valid and invalid primary keys
    test_data = [
        ("Contract_SR_Europa_3", "12345", "Direct", "01.01.2020", "01.01.2025", "01.01.2020 12:00"),  # Valid
        ("", "67890", "Direct", "01.01.2020", "01.01.2025", "01.01.2020 12:00"),  # Invalid: empty SOURCE_SYSTEM
        ("Contract_SR_Europa_3", "abc", "Direct", "01.01.2020", "01.01.2025", "01.01.2020 12:00"),  # Invalid: non-numeric ID
    ]
    
    df = spark.createDataFrame(test_data, schema=contract_schema)
    valid_df, invalid_df = validator.validate_and_cast_contract(df)
    
    assert valid_df.count() == 1  # Only first record is valid
    assert invalid_df.count() == 2  # Two invalid records
    
    # Check that invalid records have error descriptions
    invalid_rows = invalid_df.collect()
    assert all(row.validation_error is not None for row in invalid_rows)


def test_empty_dataframe_handling(spark):
    """Test graceful handling of empty dataframes."""
    validator = SchemaValidator()
    
    contract_schema = StructType([
        StructField("SOURCE_SYSTEM", StringType(), False),
        StructField("CONTRACT_ID", StringType(), False),
        StructField("CONTRACT_TYPE", StringType(), True),
        StructField("INSURED_PERIOD_FROM", StringType(), True),
        StructField("INSURED_PERIOD_TO", StringType(), True),
        StructField("CREATION_DATE", StringType(), True)
    ])
    
    # Empty dataframe with correct schema
    empty_df = spark.createDataFrame([], schema=contract_schema)
    valid_df, invalid_df = validator.validate_and_cast_contract(empty_df)
    
    # Should handle empty dataframes gracefully
    assert valid_df.count() == 0
    assert invalid_df.count() == 0

def test_handles_completely_invalid_data(spark):
    """Test graceful handling when all data is invalid."""
    validator = SchemaValidator()
    
    contract_schema = StructType([
        StructField("SOURCE_SYSTEM", StringType(), False),
        StructField("CONTRACT_ID", StringType(), False),
        StructField("CONTRACT_TYPE", StringType(), True),
        StructField("INSURED_PERIOD_FROM", StringType(), True),
        StructField("INSURED_PERIOD_TO", StringType(), True),
        StructField("CREATION_DATE", StringType(), True)
    ])
    
    # Use empty strings instead of None for non-nullable fields
    all_invalid_data = [
        ("", "abc", "Direct", "bad_date", "bad_date", "bad_datetime"),  # Empty SOURCE_SYSTEM, non-numeric CONTRACT_ID
        ("  ", "xyz", "Direct", "another_bad_date", "bad_date", "bad_datetime"),  # Whitespace SOURCE_SYSTEM, non-numeric CONTRACT_ID
        ("BadSystem", "123abc", "Direct", "2020-13-45", "2020-13-45", "invalid_timestamp"),  # Bad system name, mixed alphanumeric ID, invalid dates
    ]
    
    df = spark.createDataFrame(all_invalid_data, schema=contract_schema)
    valid_df, invalid_df = validator.validate_and_cast_contract(df)
    
    # Should handle gracefully
    assert valid_df.count() == 0
    assert invalid_df.count() == 3
    
    # All records should have error descriptions
    invalid_rows = invalid_df.collect()
    assert all(row.validation_error is not None for row in invalid_rows)
    
    # Verify different types of errors are detected
    error_messages = [row.validation_error for row in invalid_rows]
    # Should have different error types
    unique_errors = set(error_messages)
    assert len(unique_errors) >= 2  # At least 2 different error types


def test_empty_dataframe_handling(spark):
    """Test graceful handling of empty dataframes."""
    validator = SchemaValidator()
    
    contract_schema = StructType([
        StructField("SOURCE_SYSTEM", StringType(), False),
        StructField("CONTRACT_ID", StringType(), False),
        StructField("CONTRACT_TYPE", StringType(), True),
        StructField("INSURED_PERIOD_FROM", StringType(), True),
        StructField("INSURED_PERIOD_TO", StringType(), True),
        StructField("CREATION_DATE", StringType(), True)
    ])
    
    # Empty dataframe with correct schema
    empty_df = spark.createDataFrame([], schema=contract_schema)
    valid_df, invalid_df = validator.validate_and_cast_contract(empty_df)
    
    # Should handle empty dataframes gracefully
    assert valid_df.count() == 0
    assert invalid_df.count() == 0