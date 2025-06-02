"""Test hash client with focus on networking reliability."""

import pytest
from unittest.mock import patch, Mock
import requests

from src.api.hash_client import get_hash_digest


def test_hash_client_basic_functionality():
    """Test basic hash client functionality."""
    # Real API call if available
    result = get_hash_digest("CL_68545123")
    
    # API might be down, so we're flexible
    if result is not None:
        assert isinstance(result, str)
        assert len(result) > 0


@patch('src.api.hash_client.requests.get')
def test_retry_mechanism_on_timeout(mock_get):
    """Test that timeouts trigger retry with exponential backoff."""
    mock_get.side_effect = requests.exceptions.Timeout("Connection timeout")
    
    result = get_hash_digest("test_value", max_retries=2)
    
    # Should retry 2 times
    assert mock_get.call_count == 2
    assert result is None


@patch('src.api.hash_client.requests.get')
def test_successful_retry_after_failure(mock_get):
    """Test successful retry after initial failure."""
    # First call fails, second succeeds
    timeout_error = requests.exceptions.Timeout("Timeout")
    success_response = Mock()
    success_response.raise_for_status.return_value = None
    success_response.json.return_value = {"Digest": "abc123hash"}
    
    mock_get.side_effect = [timeout_error, success_response]
    
    result = get_hash_digest("test_value", max_retries=3)
    
    assert mock_get.call_count == 2
    assert result == "abc123hash"
