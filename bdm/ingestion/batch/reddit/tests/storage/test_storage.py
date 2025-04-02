"""
Tests for the storage module in the Reddit batch ingestion system.
"""
import json
from unittest import mock

import pytest
from freezegun import freeze_time

from bdm.ingestion.batch.reddit.storage import generate_filename, create_metadata, save_to_storage


class TestStorageFilenames:
    """Tests for filename generation in storage."""

    @freeze_time("2023-04-01 12:30:45")
    def test_generate_filename_with_prefix(self):
        """Test generating a filename with a prefix."""
        filename = generate_filename("askreddit", "daily")
        assert filename == "reddit/daily/askreddit/20230401_123045.json"

    @freeze_time("2023-04-01 12:30:45")
    def test_generate_filename_without_prefix(self):
        """Test generating a filename without a prefix."""
        filename = generate_filename("askreddit", "")
        assert filename == "reddit/askreddit/20230401_123045.json"


class TestStorageMetadata:
    """Tests for metadata creation in storage."""

    @staticmethod
    def test_create_metadata_with_time_filter():
        """Test creating metadata with a time filter."""
        posts = [{"id": "post1"}, {"id": "post2"}]

        # Mock the count_media_stats function to return predictable values
        with mock.patch('bdm.ingestion.batch.reddit.storage.count_media_stats') as mock_count:
            mock_count.return_value = (1, 3)  # 1 post with media, 3 total media items

            metadata = create_metadata("askreddit", "top", "week", posts)

            assert metadata["subreddit"] == "askreddit"
            assert metadata["sort_by"] == "top"
            assert metadata["time_filter"] == "week"
            assert metadata["post_count"] == 2
            assert metadata["posts_with_media"] == 1
            assert metadata["total_media_items"] == 3
            assert "scraped_at" in metadata

    @staticmethod
    def test_create_metadata_without_time_filter():
        """Test creating metadata without a time filter."""
        posts = [{"id": "post1"}, {"id": "post2"}, {"id": "post3"}]

        # Mock the count_media_stats function to return predictable values
        with mock.patch('bdm.ingestion.batch.reddit.storage.count_media_stats') as mock_count:
            mock_count.return_value = (0, 0)  # No posts with media

            metadata = create_metadata("askreddit", "new", "day", posts)

            assert metadata["subreddit"] == "askreddit"
            assert metadata["sort_by"] == "new"
            assert metadata["time_filter"] is None  # Should be None for sort_by other than 'top' or 'controversial'
            assert metadata["post_count"] == 3
            assert metadata["posts_with_media"] == 0
            assert metadata["total_media_items"] == 0
            assert "scraped_at" in metadata


class TestSaveToStorage:
    """Tests for saving data to storage."""

    @freeze_time("2023-04-01 12:30:45")
    def test_save_to_storage_success(self, mock_minio_client):
        """Test successfully saving data to storage."""
        posts = [{"id": "post1"}, {"id": "post2"}]

        # Patch get_minio_client to return our mock client
        with mock.patch('bdm.ingestion.batch.reddit.storage.get_minio_client', return_value=mock_minio_client), mock.patch('bdm.ingestion.batch.reddit.storage.generate_filename') as mock_gen_filename:
            mock_gen_filename.return_value = "reddit/daily/askreddit/20230401_123045.json"

            with mock.patch('bdm.ingestion.batch.reddit.storage.create_metadata') as mock_create_meta:
                mock_metadata = {
                    "subreddit": "askreddit",
                    "sort_by": "hot",
                    "time_filter": None,
                    "post_count": 2,
                    "posts_with_media": 0,
                    "total_media_items": 0,
                    "scraped_at": "2023-04-01T12:30:45"
                }
                mock_create_meta.return_value = mock_metadata

                save_to_storage(posts, "reddit-data", "askreddit", "hot", "", "daily")

                # Verify the MinIO client was called correctly
                mock_minio_client.put_object.assert_called_once()

                # Get the call arguments
                call_args = mock_minio_client.put_object.call_args[1]

                assert call_args["Bucket"] == "reddit-data"
                assert call_args["Key"] == "reddit/daily/askreddit/20230401_123045.json"
                assert call_args["ContentType"] == "application/json"

                # Verify the JSON data
                saved_data = json.loads(call_args["Body"])
                assert saved_data["metadata"] == mock_metadata
                assert saved_data["posts"] == posts

    @staticmethod
    def test_save_to_storage_error(mock_minio_client):
        """Test error handling when saving to storage."""
        posts = [{"id": "post1"}]

        # Patch get_minio_client to return our mock client
        with mock.patch('bdm.ingestion.batch.reddit.storage.get_minio_client', return_value=mock_minio_client):
            # Make the MinIO client raise an exception
            mock_minio_client.put_object.side_effect = Exception("Connection error")

            with pytest.raises(Exception) as exc_info:
                save_to_storage(posts, "reddit-data", "askreddit", "hot", "", "daily")

            assert "Connection error" in str(exc_info.value)
