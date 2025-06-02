from pyspark.sql import SparkSession

from src.pipeline.transformation.base_transformer import BaseTransformer
from src.pipeline.transformation.contract_claim_transformer import ContractClaimTransformer


class TransformerFactory:
    """Factory for creating data transformers."""

    @classmethod
    def get_contract_claim_transformer(cls, spark: SparkSession) -> BaseTransformer:
        """Creates a transformer for contract and claim data."""
        return ContractClaimTransformer(spark)
