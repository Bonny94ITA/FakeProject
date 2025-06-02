from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


class SchemaManager:
    """Centralized schema management for data transformations."""

    @staticmethod
    def get_contract_raw_schema() -> StructType:
        """Schema for raw CSV ingestion - everything as string first."""
        return StructType([
            StructField("SOURCE_SYSTEM", StringType(), False),
            StructField("CONTRACT_ID", StringType(), False),
            StructField("CONTRACT_TYPE", StringType(), True),
            StructField("INSURED_PERIOD_FROM", StringType(), True),
            StructField("INSURED_PERIOD_TO", StringType(), True),
            StructField("CREATION_DATE", StringType(), True)
        ])

    @staticmethod
    def get_claim_raw_schema() -> StructType:
        """Schema for raw CSV ingestion - everything as string first."""
        return StructType([
            StructField("SOURCE_SYSTEM", StringType(), False),
            StructField("CLAIM_ID", StringType(), False),
            StructField("CONTRACT_SOURCE_SYSTEM", StringType(), True),
            StructField("CONTRAT_ID", StringType(), True),  # Keep the typo for compatibility
            StructField("CLAIM_TYPE", StringType(), True),
            StructField("DATE_OF_LOSS", StringType(), True),
            StructField("AMOUNT", StringType(), True),
            StructField("CREATION_DATE", StringType(), True)
        ])

    @staticmethod
    def get_contract_validated_schema() -> StructType:
        """Schema after validation and type conversion."""
        return StructType([
            StructField("SOURCE_SYSTEM", StringType(), False),
            StructField("CONTRACT_ID", LongType(), False),
            StructField("CONTRACT_TYPE", StringType(), True),
            StructField("INSURED_PERIOD_FROM", DateType(), True),
            StructField("INSURED_PERIOD_TO", DateType(), True),
            StructField("CREATION_DATE", TimestampType(), True)
        ])

    @staticmethod
    def get_claim_validated_schema() -> StructType:
        """Schema after validation and type conversion."""
        return StructType([
            StructField("SOURCE_SYSTEM", StringType(), False),
            StructField("CLAIM_ID", StringType(), False),
            StructField("CONTRACT_SOURCE_SYSTEM", StringType(), True),
            StructField("CONTRACT_ID", LongType(), True),
            StructField("CLAIM_TYPE", IntegerType(), True),
            StructField("DATE_OF_LOSS", DateType(), True),
            StructField("AMOUNT", DecimalType(16, 5), True),
            StructField("CREATION_DATE", TimestampType(), True)
        ])

    @staticmethod
    def get_transaction_schema() -> StructType:
        """
        Returns the schema for Transaction output data based on data architect specification.

        Primary Keys: CONTRACT_SOURCE_SYSTEM, CONTRACT_SOURCE_SYSTEM_ID, NSE_ID
        Required Fields (nullable=False): TRANSACTION_TYPE, NSE_ID
        """
        return StructType([
            # Primary Key 1
            StructField("CONTRACT_SOURCE_SYSTEM", StringType(), True, {"primary_key": True}),

            # Primary Key 2
            StructField("CONTRACT_SOURCE_SYSTEM_ID", LongType(), True, {"primary_key": True}),

            # Business fields
            StructField("SOURCE_SYSTEM_ID", IntegerType(), True),
            StructField("TRANSACTION_TYPE", StringType(), False),  # NOT nullable per spec
            StructField("TRANSACTION_DIRECTION", StringType(), True),
            StructField("CONFORMED_VALUE", DecimalType(16, 5), True),
            StructField("BUSINESS_DATE", DateType(), True),
            StructField("CREATION_DATE", TimestampType(), True),
            StructField("SYSTEM_TIMESTAMP", TimestampType(), True),

            # Primary Key 3
            StructField("NSE_ID", StringType(), False, {"primary_key": True})  # NOT nullable per spec
        ])
