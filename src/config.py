import os
from pathlib import Path
from typing import Any, Dict

from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def get_config(key: str, default: str = "") -> str:
    """Get configuration value from environment."""
    return os.getenv(key, default)


def get_typed_config(key: str, value_type: type, default: Any = None) -> Any:
    """Get typed configuration value from environment."""
    value = os.getenv(key)
    if value is None:
        return default
    try:
        if value_type is bool:
            return value.lower() in ("true", "1", "yes", "on")
        return value_type(value)
    except ValueError:
        return default


def get_api_config() -> Dict[str, Any]:
    """Get API configuration."""
    return {
        "url": get_config("HASH_API_URL", "https://api.hashify.net/hash/md4/hex?value="),
        "timeout": get_typed_config("HASH_API_TIMEOUT", int, 5),
        "max_retries": get_typed_config("HASH_API_MAX_RETRIES", int, 3),
        "backoff_factor": get_typed_config("HASH_API_BACKOFF_FACTOR", int, 2),
    }


def get_spark_config() -> Dict[str, Any]:
    """Get Spark configuration."""
    return {
        "master": get_config("SPARK_MASTER", "local[*]"),
        "app_name": get_config("SPARK_APP_NAME", "DataTransformer"),
        "enable_caching": get_typed_config("ENABLE_CACHING", bool, True),
    }


def get_schema_config() -> Dict[str, Any]:
    """Get schema validation configuration."""
    return {
        "validation_mode": get_config("SCHEMA_VALIDATION_MODE", "STRICT"),
        "evolution_enabled": get_typed_config("SCHEMA_EVOLUTION_ENABLED", bool, False),
    }


def get_file_format_config() -> Dict[str, Any]:
    """Get file format configuration."""
    return {
        "input_format": get_config("INPUT_FILE_FORMAT", "csv"),
        "output_format": get_config("OUTPUT_FILE_FORMAT", "parquet"),
        "csv_delimiter": get_config("CSV_DELIMITER", ","),
        "csv_quote_char": get_config("CSV_QUOTE_CHAR", '"'),
        "csv_escape_char": get_config("CSV_ESCAPE_CHAR", '"'),
    }


def get_csv_options() -> Dict[str, str]:
    """Get CSV reading options for Spark."""
    return {
        "header": get_config("CSV_HEADER", "true"),
        "sep": get_config("CSV_DELIMITER", ","),
        "quote": get_config("CSV_QUOTE_CHAR", '"'),
        "escape": get_config("CSV_ESCAPE_CHAR", '"'),
        "mode": get_config("CSV_MODE", "PERMISSIVE"),
        "multiLine": get_config("CSV_MULTILINE", "true"),
        "ignoreLeadingWhiteSpace": "true",
        "ignoreTrailingWhiteSpace": "true",
    }


def get_path_config() -> Dict[str, str]:
    """Get path configuration."""
    return {
        "input_path": get_config("INPUT_PATH", "./data/input"),
        "output_path": get_config("OUTPUT_PATH", "./data/output"),
    }


def join_paths(*args) -> Path:
    """Join paths in a cross-platform way."""
    return Path(*args)
