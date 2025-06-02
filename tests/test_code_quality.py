"""Essential code quality tests for technical interview."""
import os
import pytest
from pyspark.sql import SparkSession

from src.pipeline.contract_pipeline import ContractClaimPipeline
from src.pipeline.extraction.extractor_factory import DataExtractorFactory
from src.pipeline.loading.loader_factory import LoaderFactory
from src.config import get_path_config


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[*]").appName("TestApp").getOrCreate()


def test_factory_pattern_works(spark):
    """Test that factory pattern creates different instances correctly."""
    # Test extractor factory
    contract_extractor1 = DataExtractorFactory.create_extractor(spark)
    contract_extractor2 = DataExtractorFactory.create_extractor(spark)
    claim_extractor = DataExtractorFactory.create_extractor(spark)
    
    # Different instances but same type for same factory method
    assert contract_extractor1 is not contract_extractor2
    assert type(contract_extractor1) == type(contract_extractor2)
    assert type(claim_extractor) == type(contract_extractor1)

    # Test loader factory
    loader1 = LoaderFactory.create_loader("/tmp/output1")
    loader2 = LoaderFactory.create_loader("/tmp/output2")
    
    # Different instances but same type
    assert loader1 is not loader2
    assert type(loader1) == type(loader2)


def test_dependency_injection(spark):
    """Test that components are properly injected."""
    pipeline = ContractClaimPipeline(spark, "/tmp/input", "/tmp/output")
    
    # Verify all components are injected
    assert pipeline.contract_extractor is not None
    assert pipeline.claim_extractor is not None
    assert pipeline.schema_validator is not None
    assert pipeline.transformer is not None
    assert pipeline.loader is not None


def test_configuration_management():
    """Test that configuration is properly loaded."""

    # Test environment variables (direct access)
    hash_url = os.getenv("HASH_API_URL")
    input_path = os.getenv("INPUT_PATH")
    
    assert hash_url is not None
    assert "hashify.net" in hash_url
    assert input_path is not None
    
    # Test config functions
    path_config = get_path_config()
    assert "input_path" in path_config
    assert "output_path" in path_config