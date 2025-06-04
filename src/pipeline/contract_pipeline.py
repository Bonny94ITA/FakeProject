import logging
from typing import Dict, Tuple

from pyspark.sql import DataFrame, SparkSession

from src.config import join_paths
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

        # Create components using factory pattern - metodi unificati
        self.contract_extractor = DataExtractorFactory.create_extractor(spark)
        self.claim_extractor = DataExtractorFactory.create_extractor(spark)
        self.schema_validator = SchemaValidator()
        self.transformer = TransformerFactory.get_contract_claim_transformer(spark)
        self.loader = LoaderFactory.create_loader(output_dir)

    def extract(self) -> Dict[str, DataFrame]:
        """
        Extract contract and claim data using specialized extractors.
        Returns a dictionary containing both types of data frames.
        """
        try:
            contract_file = join_paths(self.input_dir, "Contract.csv")
            claim_file = join_paths(self.input_dir, "Claim.csv")

            if not contract_file.exists():
                self.logger.error(f"Contract file not found: {contract_file}")
                raise FileNotFoundError(f"Contract file not found: {contract_file}")
            if not claim_file.exists():
                self.logger.error(f"Claim file not found: {claim_file}")
                raise FileNotFoundError(f"Claim file not found: {claim_file}")

            try:
                contract_df = self.contract_extractor.extract(str(contract_file))
            except Exception as e:
                self.logger.error(f"Error extracting contract data: {e}")
                raise ValueError(f"Failed to extract contract data: {e}")

            try:
                claim_df = self.claim_extractor.extract(str(claim_file))
            except Exception as e:
                self.logger.error(f"Error extracting claim data: {e}")
                raise ValueError(f"Failed to extract claim data: {e}")

            if contract_df.count() > 0:
                contract_df = contract_df.cache()
            if claim_df.count() > 0:
                claim_df = claim_df.cache()

            self.logger.info(f"Extracted {contract_df.count()} contracts and {claim_df.count()} claims")
            return {"contract": contract_df, "claim": claim_df}

        except FileNotFoundError as e:
            self.logger.error(f"File not found during extraction: {e}")
            raise
        except ValueError as e:
            self.logger.error(f"Value error during extraction: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error during data extraction: {e}")
            raise

    def validate(self, data: Dict[str, DataFrame]) -> Tuple[Dict[str, DataFrame], Dict[str, DataFrame]]:
        """Enhanced validation with schema validation and type conversion."""
        try:
            contract_df = data["contract"]
            claim_df = data["claim"]

            valid_contracts, invalid_contracts = self.schema_validator.validate_dataframe(contract_df, "contract")
            valid_claims, invalid_claims = self.schema_validator.validate_dataframe(claim_df, "claim")

            total_invalid = invalid_contracts.count() + invalid_claims.count()
            if total_invalid > 0:
                self.logger.warning(f"Found {total_invalid} invalid rows (schema validation)")

            return (
                {"contract": valid_contracts, "claim": valid_claims},
                {"invalid_contracts": invalid_contracts, "invalid_claims": invalid_claims}
            )
        except Exception as e:
            self.logger.error(f"Error during validation: {e}")
            raise

    def transform(self, data: Dict[str, DataFrame]) -> DataFrame:
        """Transform data according to business rules using the transformer."""
        try:
            return self.transformer.transform(data)
        except Exception as e:
            self.logger.error(f"Error during transformation: {e}")
            raise

    def load(self, data: DataFrame) -> None:
        """Load data using the configured loader."""
        try:
            self.loader.load(data, "TRANSACTIONS")
        except Exception as e:
            self.logger.error(f"Error during loading: {e}")
            raise
