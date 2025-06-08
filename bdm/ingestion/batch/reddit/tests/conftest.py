"""
Pytest configuration file for Reddit scraper tests.
"""
from unittest import mock

import pytest


@pytest.fixture
def mock_minio_client():
    """Creates a mock for the MinIO client."""
    with mock.patch('bdm.finnhub.batch.utils.get_minio_client') as mock_client:
        # Create a mock S3 client
        mock_s3 = mock.Mock()
        mock_client.return_value = mock_s3
        yield mock_s3


@pytest.fixture
def mock_requests_get():
    """Creates a mock for requests.get."""
    with mock.patch('requests.get') as mock_get:
        # Create a mock response object
        mock_response = mock.Mock()
        mock_response.content = b'test_content'
        mock_response.raise_for_status = mock.Mock()
        mock_get.return_value = mock_response
        yield mock_get, mock_response


@pytest.fixture
def mock_uuid():
    """Creates a mock for uuid.uuid4."""
    with mock.patch('uuid.uuid4') as mock_uuid:
        mock_uuid.return_value = 'test_uuid'
        yield mock_uuid
