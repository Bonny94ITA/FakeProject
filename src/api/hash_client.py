import logging
import time
from typing import Optional
from urllib.parse import quote

import requests

from src.config import get_hash_api_config

logger = logging.getLogger(__name__)


def get_hash_digest(value: str, max_retries: Optional[int] = None) -> Optional[str]:
    """
    Gets a hash digest for the provided value with simple retry logic.

    Args:
        value: The value to hash
        max_retries: Override default max retries

    Returns:
        The hash digest or None in case of error
    """
    if not value:
        logger.warning("Empty value provided for hashing")
        return None

    # Get configuration
    config = get_hash_api_config()
    url = f"{config['url']}{quote(value)}"
    max_retries = max_retries or config["max_retries"]

    for attempt in range(max_retries):
        try:
            logger.debug(f"Hash API call attempt {attempt + 1}: {value}")

            response = requests.get(url, timeout=config["timeout"])
            response.raise_for_status()

            data = response.json()
            digest = data.get("Digest")

            if digest:
                return digest
            else:
                logger.warning("API returned empty digest")
                return None

        except requests.exceptions.Timeout:
            _log_retry(attempt + 1, max_retries, "Timeout")

        except requests.exceptions.HTTPError as e:
            if e.response and e.response.status_code >= 500:
                # Server error - retry
                _log_retry(attempt + 1, max_retries, f"Server error {e.response.status_code}")
            else:
                # Client error - don't retry
                logger.error(f"Client error {e.response.status_code if e.response else 'unknown'}")
                return None

        except (requests.RequestException, ValueError) as e:
            logger.error(f"API request failed: {str(e)}")
            return None

        # Wait before retry (except on last attempt)
        if attempt < max_retries - 1:
            wait_time = config["backoff_factor"] ** attempt
            time.sleep(wait_time)

    logger.error(f"Hash API failed after {max_retries} attempts")
    return None


def _log_retry(attempt: int, max_attempts: int, reason: str) -> None:
    """Helper function to log retry attempts."""
    if attempt < max_attempts:
        logger.warning(f"{reason}. Attempt {attempt}/{max_attempts}, retrying...")
    else:
        logger.error(f"{reason}. Final attempt {attempt}/{max_attempts} failed")
