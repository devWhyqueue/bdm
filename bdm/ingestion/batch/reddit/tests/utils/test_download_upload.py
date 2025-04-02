import uuid
from unittest import mock

import requests

from bdm.ingestion.batch.reddit.media_utils import (
    download_media,
    get_s3_key,
    upload_media_to_s3
)


class TestDownloadAndUpload:
    """Tests for media download and upload functions."""

    @mock.patch('uuid.uuid4')
    @mock.patch('requests.get')
    def test_download_media_success(self, mock_get, mock_uuid):
        """Test successful media download."""
        # Setup mocks
        mock_uuid.return_value = uuid.UUID('12345678123456781234567812345678')
        mock_response = mock.Mock()
        # Create content with more than 100 bytes to pass the size check
        mock_response.content = b'test_content' * 15  # 132 bytes
        mock_response.raise_for_status = mock.Mock()
        mock_get.return_value = mock_response

        # Call function
        result = download_media("https://example.com/image.jpg", "image")

        # Assert
        assert result is not None
        filename, content_type, content = result
        assert filename == "12345678123456781234567812345678.jpg"
        assert content_type == "image/jpeg"
        assert content == b'test_content' * 15
        mock_get.assert_called_once_with("https://example.com/image.jpg", timeout=30)

    @mock.patch('requests.get')
    def test_download_media_too_small(self, mock_get):
        """Test handling of suspiciously small downloads."""
        # Setup mock
        mock_response = mock.Mock()
        mock_response.content = b'x'  # Very small content
        mock_response.raise_for_status = mock.Mock()
        mock_get.return_value = mock_response

        # Call function
        result = download_media("https://example.com/image.jpg", "image")

        # Assert
        assert result is None

    @mock.patch('requests.get')
    def test_download_media_http_error(self, mock_get):
        """Test handling of HTTP errors."""
        # Setup mock to raise an exception
        mock_response = mock.Mock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Not Found")
        mock_get.return_value = mock_response

        # Call function
        result = download_media("https://example.com/image.jpg", "image")

        # Assert
        assert result is None

    @mock.patch('requests.get')
    def test_download_media_connection_error(self, mock_get):
        """Test handling of connection errors."""
        # Setup mock to raise connection error
        mock_get.side_effect = requests.exceptions.ConnectionError("Connection failed")

        # Call function
        result = download_media("https://example.com/image.jpg", "image")

        # Assert
        assert result is None

    @staticmethod
    def test_get_s3_key_with_prefix():
        """Test S3 key generation with prefix."""
        key = get_s3_key("testsubreddit", "testprefix", "image", "testfile.jpg")
        assert key == "reddit/testprefix/testsubreddit/images/testfile.jpg"

    @staticmethod
    def test_get_s3_key_without_prefix():
        """Test S3 key generation without prefix."""
        key = get_s3_key("testsubreddit", "", "video", "testvideo.mp4")
        assert key == "reddit/testsubreddit/videos/testvideo.mp4"

    @mock.patch('bdm.ingestion.batch.reddit.media_utils.get_minio_client')
    def test_upload_media_to_s3_success(self, mock_get_client):
        """Test successful S3 upload."""
        # Setup mock
        mock_s3 = mock.Mock()
        mock_get_client.return_value = mock_s3

        # Call function
        result = upload_media_to_s3(
            "testbucket", "testsubreddit", "testprefix",
            "image", "testfile.jpg", b'test_content', "image/jpeg"
        )

        # Assert
        assert result == "reddit/testprefix/testsubreddit/images/testfile.jpg"
        mock_s3.put_object.assert_called_once_with(
            Bucket="testbucket",
            Key="reddit/testprefix/testsubreddit/images/testfile.jpg",
            Body=b'test_content',
            ContentType="image/jpeg"
        )

    @mock.patch('bdm.ingestion.batch.reddit.media_utils.get_minio_client')
    def test_upload_media_to_s3_error(self, mock_get_client):
        """Test handling of S3 upload errors."""
        # Setup mock to raise an exception
        mock_s3 = mock.Mock()
        mock_s3.put_object.side_effect = Exception("S3 error")
        mock_get_client.return_value = mock_s3

        # Call function
        result = upload_media_to_s3(
            "testbucket", "testsubreddit", "testprefix",
            "image", "testfile.jpg", b'test_content', "image/jpeg"
        )

        # Assert
        assert result is None
