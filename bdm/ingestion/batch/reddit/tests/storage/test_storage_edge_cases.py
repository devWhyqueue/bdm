"""
Tests for edge cases and error handling in the storage module.
"""
from unittest import mock

import pytest

from bdm.ingestion.batch.reddit.storage import generate_filename, create_metadata, save_to_storage


class TestStorageEdgeCases:
    """Tests for edge cases in storage functionality."""

    @staticmethod
    def test_generate_filename_special_characters():
        """Test generating a filename with special characters in subreddit name."""
        filename = generate_filename("ask_reddit-NSFW", "daily")
        assert "ask_reddit-NSFW" in filename
        assert filename.startswith("reddit/daily/")
        assert filename.endswith(".json")

    @staticmethod
    def test_generate_filename_unicode():
        """Test generating a filename with Unicode characters in subreddit name."""
        filename = generate_filename("日本語", "")
        assert "日本語" in filename
        assert filename.startswith("reddit/")
        assert filename.endswith(".json")

    @staticmethod
    def test_create_metadata_empty_posts():
        """Test creating metadata with an empty posts list."""
        posts = []

        # Mock the count_media_stats function
        with mock.patch('bdm.finnhub.batch.reddit.storage.count_media_stats') as mock_count:
            mock_count.return_value = (0, 0)

            metadata = create_metadata("askreddit", "hot", "", posts)

            assert metadata["subreddit"] == "askreddit"
            assert metadata["sort_by"] == "hot"
            assert metadata["time_filter"] is None
            assert metadata["post_count"] == 0
            assert metadata["posts_with_media"] == 0
            assert metadata["total_media_items"] == 0

    @staticmethod
    def test_create_metadata_controversial():
        """Test creating metadata with controversial sort and time filter."""
        posts = [{"id": "post1"}]

        with mock.patch('bdm.finnhub.batch.reddit.storage.count_media_stats') as mock_count:
            mock_count.return_value = (0, 0)

            metadata = create_metadata("askreddit", "controversial", "all", posts)

            assert metadata["subreddit"] == "askreddit"
            assert metadata["sort_by"] == "controversial"
            assert metadata["time_filter"] == "all"  # Should keep time_filter for controversial

    @staticmethod
    def test_save_to_storage_client_not_initialized():
        """Test handling the case where the MinIO client fails to initialize."""
        posts = [{"id": "post1"}]

        with mock.patch('bdm.finnhub.batch.reddit.storage.get_minio_client') as mock_get_client:
            # Simulate the client raising a ValueError (missing credentials)
            mock_get_client.side_effect = ValueError("MinIO credentials not found")

            with pytest.raises(ValueError) as exc_info:
                save_to_storage(posts, "reddit-data", "askreddit", "hot", "", "daily")

            assert "MinIO credentials not found" in str(exc_info.value)

    @staticmethod
    def test_save_to_storage_with_empty_posts(mock_minio_client):
        """Test saving with an empty posts list."""
        posts = []

        with mock.patch('bdm.finnhub.batch.reddit.storage.get_minio_client', return_value=mock_minio_client):
            save_to_storage(posts, "reddit-data", "askreddit", "hot", "", "daily")

            # Verify the MinIO client was called
            mock_minio_client.put_object.assert_called_once()

            # Get the body content
            body = mock_minio_client.put_object.call_args[1]["Body"]
            assert '"posts": []' in body  # Ensure empty posts list is properly saved

    @staticmethod
    def test_save_to_storage_logging(mock_minio_client):
        """Test that successful saves are logged properly."""
        posts = [{"id": "post1"}]

        with mock.patch('bdm.finnhub.batch.reddit.storage.get_minio_client',
                        return_value=mock_minio_client), mock.patch(
                'bdm.finnhub.batch.reddit.storage.logger') as mock_logger:
            save_to_storage(posts, "reddit-data", "askreddit", "hot", "", "")

            # Verify that the success was logged
            mock_logger.info.assert_called_once()
            log_message = mock_logger.info.call_args[0][0]
            assert "Successfully saved data" in log_message

    @staticmethod
    def test_save_to_storage_error_logging(mock_minio_client):
        """Test that errors during save are logged properly."""
        posts = [{"id": "post1"}]

        with mock.patch('bdm.finnhub.batch.reddit.storage.get_minio_client', return_value=mock_minio_client):
            mock_minio_client.put_object.side_effect = RuntimeError("S3 connection error")

            with mock.patch('bdm.finnhub.batch.reddit.storage.logger') as mock_logger:
                with pytest.raises(RuntimeError):
                    save_to_storage(posts, "reddit-data", "askreddit", "hot", "", "")

                # Verify that the error was logged
                mock_logger.error.assert_called_once()
                error_message = mock_logger.error.call_args[0][0]
                assert "Error saving to storage" in error_message
