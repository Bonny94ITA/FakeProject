"""Configuration module for the data pipeline."""

import json
import os
from pathlib import Path
from typing import Any, Dict

from dotenv import load_dotenv

load_dotenv()

def load_application_config() -> Dict[str, Any]:
    """Load application configuration from JSON file."""
    config_path = Path(__file__).parent.parent / "config" / "application.json"

    if config_path.exists():
        with open(config_path, "r") as f:
            return json.load(f)
    else:
        return {}

# Load app config once
APP_CONFIG = load_application_config()

def get_hash_api_config() -> Dict[str, Any]:
    """Get hash API configuration."""
    return {
        "url": os.getenv("HASH_API_URL", "https://api.hashify.net/hash/md4/hex?value="),
        "timeout": int(os.getenv("HASH_API_TIMEOUT", "5")),
        "max_retries": int(os.getenv("HASH_API_MAX_RETRIES", "3")),
        "backoff_factor": float(os.getenv("HASH_API_BACKOFF_FACTOR", "2")),
    }

def get_spark_config() -> Dict[str, Any]:
    """Get Spark configuration."""
    return {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.shuffle.partitions": "4",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    }

def get_file_format_config() -> Dict[str, Any]:
    """Get file format configuration."""
    formats = APP_CONFIG.get("file_formats", {})
    return {
        "destination": formats.get("destination", os.getenv("DESTINATION", "file")),
        "input_format": formats.get("input_format", os.getenv("INPUT_FILE_FORMAT", "csv")),
        "output_format": formats.get("output_format", os.getenv("OUTPUT_FILE_FORMAT", "csv")),
    }

def get_csv_options() -> Dict[str, str]:
    """Get CSV reading options for Spark."""
    csv_opts = APP_CONFIG.get("csv_options", {})
    return {
        "header": csv_opts.get("header", os.getenv("CSV_HEADER", "true")),
        "sep": csv_opts.get("delimiter", os.getenv("CSV_DELIMITER", ",")),
        "quote": csv_opts.get("quote_char", os.getenv("CSV_QUOTE_CHAR", '"')),
        "escape": csv_opts.get("escape_char", os.getenv("CSV_ESCAPE_CHAR", '"')),
        "mode": csv_opts.get("mode", os.getenv("CSV_MODE", "PERMISSIVE")),
        "multiLine": csv_opts.get("multiline", os.getenv("CSV_MULTILINE", "true")),
        "ignoreLeadingWhiteSpace": "true",
        "ignoreTrailingWhiteSpace": "true",
    }

def get_path_config() -> Dict[str, str]:
    """Get path configuration from environment variables."""
    return {
        "input_path": os.getenv("INPUT_PATH", "data/input"),
        "output_path": os.getenv("OUTPUT_PATH", "data/output")
    }

def join_paths(*args) -> Path:
    """Join paths in a cross-platform way."""
    return Path(*args)

def get_format_write_options() -> Dict[str, Dict[str, str]]:
    """Get write options for all supported formats."""
    return {
        "csv": {
            "header": "true",
            "nullValue": "",
            "timestampFormat": "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
            "dateFormat": "yyyy-MM-dd",
        },
        "json": {
            "ignoreNullFields": "false",
            "timestampFormat": "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
            "dateFormat": "yyyy-MM-dd",
        },
        "parquet": {
            "timestampFormat": "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
            "dateFormat": "yyyy-MM-dd",
        },
        "delta": {
            "timestampFormat": "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
            "dateFormat": "yyyy-MM-dd",
        },
        "orc": {
            "timestampFormat": "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
            "dateFormat": "yyyy-MM-dd",
        }
    }
