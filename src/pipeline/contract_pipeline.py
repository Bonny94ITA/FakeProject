import logging
from typing import Dict, Tuple

from pyspark.sql import DataFrame, SparkSession

from src.config import get_file_format_config, join_paths
from src.pipeline.data_pipeline import DataPipeline
from src.pipeline.extraction.extractor_factory import DataExtractorFactory
from src.pipeline.loading.loader_factory import LoaderFactory
from src.pipeline.transformation.transformer_factory import TransformerFactory
from src.pipeline.validation.schema_validator import SchemaValidator


class ContractClaimPipeline(DataPipeline):
    """Pipeline for transforming contracts and claims into transactions."""

    def __init__(self, spark: SparkSession, input_dir: str, output_dir: str):
        super().__init__(spark, input_dir, output_dir)
        self.logger = logging.getLogger(self.__class__.__name__)

        # Get file format configuration
        self.file_format_config = get_file_format_config()

        # Create components using factory pattern - metodi unificati
        self.contract_extractor = DataExtractorFactory.create_extractor(spark, "contract")
        self.claim_extractor = DataExtractorFactory.create_extractor(spark, "claim")
        self.schema_validator = SchemaValidator()
        self.transformer = TransformerFactory.get_contract_claim_transformer(spark)
        self.loader = LoaderFactory.create_loader(output_dir, config=self.file_format_config)

    def extract(self) -> Dict[str, DataFrame]:
        """
        Extract contract and claim data using specialized extractors.
        Returns a dictionary containing both types of data frames.
        """
        try:
            # Use robust path joining for both files to ensure cross-platform compatibility
            contract_file = join_paths(self.input_dir, "Contract.csv")
            claim_file = join_paths(self.input_dir, "Claim.csv")

            # Extract data using the appropriate extractors
            contract_df = self.contract_extractor.extract(str(contract_file))
            claim_df = self.claim_extractor.extract(str(claim_file))

            # Cache the DataFrames for better performance
            contract_df = contract_df.cache()
            claim_df = claim_df.cache()

            self.logger.info(f"Extracted {contract_df.count()} contracts and {claim_df.count()} claims")
            return {"contract": contract_df, "claim": claim_df}

        except Exception as e:
            self.logger.error(f"Error during data extraction: {str(e)}")
            raise

    def validate(self, data: Dict[str, DataFrame]) -> Tuple[Dict[str, DataFrame], Dict[str, DataFrame]]:
        """Enhanced validation with schema validation and type conversion."""
        # Get raw input data
        contract_df = data["contract"]
        claim_df = data["claim"]

        # Validate data using the schema validator
        valid_contracts, invalid_contracts = self.schema_validator.validate_and_cast_contract(contract_df)
        valid_claims, invalid_claims = self.schema_validator.validate_and_cast_claim(claim_df)

        total_invalid = invalid_contracts.count() + invalid_claims.count()
        if total_invalid > 0:
            self.logger.warning(f"Found {total_invalid} invalid rows (schema validation)")

        return (
            {"contract": valid_contracts, "claim": valid_claims},
            {"invalid_contracts": invalid_contracts, "invalid_claims": invalid_claims}
        )

    def transform(self, data: Dict[str, DataFrame]) -> DataFrame:
        """Transform data according to business rules using the transformer."""
        # Delegate transformation to the specialized transformer
        return self.transformer.transform(data)

    def load(self, data: DataFrame) -> None:
        """Load data using the configured loader."""
        # Delegate loading to the specialized loader
        self.loader.load(data, "TRANSACTIONS")
