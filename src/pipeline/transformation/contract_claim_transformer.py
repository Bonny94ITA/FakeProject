from typing import Dict, List

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lit, regexp_replace, to_date, to_timestamp, udf, when
from pyspark.sql.types import StringType

from src.api.hash_client import get_hash_digest
from src.pipeline.transformation.base_transformer import BaseTransformer


class ContractClaimTransformer(BaseTransformer):
    """Transforms contract and claim data into standardized transactions."""

    def transform(self, data: Dict[str, DataFrame]) -> DataFrame:
        """Transform contract and claim data according to business rules."""
        contract_df = data["contract"]
        claim_df = data["claim"]

        # Prepare dataframes for joining
        contract_df, claim_df = self._prepare_dataframes(contract_df, claim_df)

        # Join the dataframes
        df = self._join_dataframes(contract_df, claim_df)

        # Apply business transformations
        df = self._apply_business_transformations(df)

        # Generate NSE_ID using hash API
        df = self._generate_nse_id(df)

        # Select only required columns
        result_df = self._select_output_columns(df)

        self.logger.info(f"Transformation completed. Generated {result_df.count()} transactions")
        return result_df

    def _prepare_dataframes(self, contract_df: DataFrame, claim_df: DataFrame) -> tuple[DataFrame, DataFrame]:
        """Prepare dataframes for joining by renaming columns and handling typos."""
        # Rename ambiguous columns to avoid conflicts
        contract_df = contract_df.withColumnRenamed("CREATION_DATE", "CONTRACT_CREATION_DATE")
        claim_df = claim_df.withColumnRenamed("CREATION_DATE", "CLAIM_CREATION_DATE")

        # Rename CONTRACT_ID to distinguish the two sources
        contract_df = contract_df.withColumnRenamed("CONTRACT_ID", "CONTRACT_ID_FROM_CONTRACT")
        claim_df = claim_df.withColumnRenamed("CONTRACT_ID", "CONTRACT_ID_FROM_CLAIM")

        return contract_df, claim_df

    def _join_dataframes(self, contract_df: DataFrame, claim_df: DataFrame) -> DataFrame:
        """Join contract and claim dataframes based on business keys."""
        return claim_df.join(
            contract_df,
            (claim_df["CONTRACT_SOURCE_SYSTEM"] == contract_df["SOURCE_SYSTEM"]) &
            (claim_df["CONTRACT_ID_FROM_CLAIM"] == contract_df["CONTRACT_ID_FROM_CONTRACT"]),
            how="left"
        )

    def _apply_business_transformations(self, df: DataFrame) -> DataFrame:
        """Apply all business transformations and mappings."""
        # CONTRACT_SOURCE_SYSTEM
        df = df.withColumn("CONTRACT_SOURCE_SYSTEM", lit("Europe 3"))

        # CONTRACT_SOURCE_SYSTEM_ID - uses the specific column
        df = df.withColumn("CONTRACT_SOURCE_SYSTEM_ID",
                      col("CONTRACT_ID_FROM_CONTRACT").cast("long"))

        # Extract SOURCE_SYSTEM_ID - remove prefix and convert to integer
        df = df.withColumn(
            "SOURCE_SYSTEM_ID",
            regexp_replace(col("CLAIM_ID"), "^[A-Za-z_]+", "").cast("int")
        )

        # Map TRANSACTION_TYPE - ensure NOT NULL per data architect spec
        df = df.withColumn(
            "TRANSACTION_TYPE",
            when(col("CLAIM_TYPE") == "1", "Private")
            .when(col("CLAIM_TYPE") == "2", "Corporate")
            .otherwise("Unknown")
        )

        # Map TRANSACTION_DIRECTION using like
        df = df.withColumn(
            "TRANSACTION_DIRECTION",
            when(col("CLAIM_ID").like("CL%"), "COINSURANCE")
            .when(col("CLAIM_ID").like("RX%"), "REINSURANCE")
            .otherwise(None)  # Can be NULL per spec
        )

        # CONFORMED_VALUE
        df = df.withColumn("CONFORMED_VALUE", col("AMOUNT").cast("decimal(16,5)"))

        # BUSINESS_DATE
        df = df.withColumn(
            "BUSINESS_DATE",
            to_date(col("DATE_OF_LOSS"), "dd.MM.yyyy")
        )

        # CREATION_DATE - uses the specific column
        df = df.withColumn(
            "CREATION_DATE",
            to_timestamp(col("CLAIM_CREATION_DATE"), "dd.MM.yyyy HH:mm")
        )

        # SYSTEM_TIMESTAMP
        df = df.withColumn("SYSTEM_TIMESTAMP", current_timestamp())

        return df

    def _generate_nse_id(self, df: DataFrame) -> DataFrame:
        """Generate NSE_ID using hash API for each unique claim ID."""
        # NSE_ID - optimization for batch API calls
        distinct_claim_ids = [row.CLAIM_ID for row in df.select("CLAIM_ID").distinct().collect() if row.CLAIM_ID is not None]
        self.logger.info(f"Calculating hash for {len(distinct_claim_ids)} unique claim IDs")

        # Create a hash dictionary for all claim IDs
        hash_map = self._create_hash_map(distinct_claim_ids)

        # Broadcast the dictionary to all Spark workers
        hash_map_broadcast = self.spark.sparkContext.broadcast(hash_map)

        # UDF that uses the local map for fast lookup
        def get_hash_from_map(claim_id):
            if claim_id is None:
                return "UNKNOWN_HASH"  # Default to ensure NOT NULL per spec
            hash_value = hash_map_broadcast.value.get(claim_id, "") # Lookup in the broadcasted map
            return hash_value if hash_value else "UNKNOWN_HASH"  # Ensure NOT NULL values

        hash_udf = udf(get_hash_from_map, StringType())
        return df.withColumn("NSE_ID", hash_udf(col("CLAIM_ID")))

    def _create_hash_map(self, claim_ids: List[str]) -> Dict[str, str]:
        """Create a hash map for all claim IDs."""
        hash_map = {}
        for claim_id in claim_ids:
            try:
                hash_result = get_hash_digest(claim_id) # One API call per claim ID
                hash_map[claim_id] = hash_result if hash_result else "UNKNOWN_HASH"
            except Exception as e:
                self.logger.error(f"Error calling API for hash of {claim_id}: {str(e)}")
                hash_map[claim_id] = "UNKNOWN_HASH"
        return hash_map

    def _select_output_columns(self, df: DataFrame) -> DataFrame:
        """Select only required columns for the output in the exact order specified by data architect."""
        columns = [
            "CONTRACT_SOURCE_SYSTEM",      # PK1 - string, nullable: true
            "CONTRACT_SOURCE_SYSTEM_ID",   # PK2 - long, nullable: true
            "SOURCE_SYSTEM_ID",            # integer, nullable: true
            "TRANSACTION_TYPE",            # string, nullable: false (NOT NULL)
            "TRANSACTION_DIRECTION",       # string, nullable: true
            "CONFORMED_VALUE",             # decimal(16,5), nullable: true
            "BUSINESS_DATE",               # date, nullable: true
            "CREATION_DATE",               # timestamp, nullable: true
            "SYSTEM_TIMESTAMP",            # timestamp, nullable: true
            "NSE_ID"                       # PK3 - string, nullable: false (NOT NULL)
        ]
        return df.select(*columns)
