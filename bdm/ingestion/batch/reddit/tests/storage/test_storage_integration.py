"""
Integration tests for the storage module in the Reddit batch ingestion system.
"""
import json
from unittest import mock

from freezegun import freeze_time

from bdm.ingestion.batch.reddit.storage import save_to_storage


@freeze_time("2023-04-01 12:30:45")
class TestStorageIntegration:
    """Integration tests for the storage module."""

    @staticmethod
    def test_end_to_end_data_storage(mock_minio_client):
        """Test the end-to-end flow of saving Reddit data to storage."""
        # Sample Reddit posts data
        sample_posts = [
            {
                "id": "post1",
                "title": "First post",
                "author": "user1",
                "media": [
                    {
                        "media_type": "image",
                        "source_url": "https://i.redd.it/image1.jpg",
                        "s3_url": "reddit/images/image1.jpg",
                        "filename": "image1.jpg",
                        "content_type": "image/jpeg"
                    }
                ]
            },
            {
                "id": "post2",
                "title": "Second post",
                "author": "user2",
                "media": []
            },
            {
                "id": "post3",
                "title": "Third post with gallery",
                "author": "user3",
                "media": [
                    {
                        "media_type": "image",
                        "source_url": "https://i.redd.it/gallery1.jpg",
                        "s3_url": "reddit/images/gallery1.jpg",
                        "filename": "gallery1.jpg",
                        "content_type": "image/jpeg",
                        "gallery_item_id": "1"
                    },
                    {
                        "media_type": "image",
                        "source_url": "https://i.redd.it/gallery2.jpg",
                        "s3_url": "reddit/images/gallery2.jpg",
                        "filename": "gallery2.jpg",
                        "content_type": "image/jpeg",
                        "gallery_item_id": "2"
                    }
                ]
            }
        ]

        # Save the data to storage
        with mock.patch('bdm.ingestion.batch.reddit.storage.get_minio_client', return_value=mock_minio_client):
            # Mock the media stats function to avoid any actual processing
            with mock.patch('bdm.ingestion.batch.reddit.storage.count_media_stats') as mock_count:
                mock_count.return_value = (2, 3)  # 2 posts with media, 3 total media items

                save_to_storage(
                    posts=sample_posts,
                    bucket="reddit-data",
                    subreddit="datascience",
                    sort_by="top",
                    time_filter="month",
                    prefix="weekly"
                )

                # Verify the MinIO client was called correctly
                mock_minio_client.put_object.assert_called_once()

                # Get the call arguments
                call_args = mock_minio_client.put_object.call_args[1]

                # Check basic parameters
                assert call_args["Bucket"] == "reddit-data"
                assert call_args["Key"].startswith("reddit/weekly/datascience/")
                assert call_args["Key"].endswith(".json")
                assert call_args["ContentType"] == "application/json"

                # Parse and verify the saved data
                saved_data = json.loads(call_args["Body"])

                # Check metadata
                metadata = saved_data["metadata"]
                assert metadata["subreddit"] == "datascience"
                assert metadata["sort_by"] == "top"
                assert metadata["time_filter"] == "month"
                assert metadata["post_count"] == 3
                assert metadata["posts_with_media"] == 2  # 2 posts have media
                assert metadata["total_media_items"] == 3  # 3 total media items
                assert metadata["scraped_at"].startswith("2023-04-01T12:30:45")

                # Check posts data
                assert saved_data["posts"] == sample_posts

    @staticmethod
    def test_integration_with_empty_data(mock_minio_client):
        """Test integration with empty data."""
        # Empty posts list
        empty_posts = []

        # Save the empty data
        with mock.patch('bdm.ingestion.batch.reddit.storage.get_minio_client', return_value=mock_minio_client):
            # Mock the media stats function
            with mock.patch('bdm.ingestion.batch.reddit.storage.count_media_stats') as mock_count:
                mock_count.return_value = (0, 0)  # No posts with media

                save_to_storage(
                    posts=empty_posts,
                    bucket="reddit-data",
                    subreddit="empty",
                    sort_by="hot",
                    time_filter="",
                    prefix=""
                )

                # Verify the MinIO client was still called
                mock_minio_client.put_object.assert_called_once()

                # Get and parse the saved data
                call_args = mock_minio_client.put_object.call_args[1]
                saved_data = json.loads(call_args["Body"])

                # Check metadata with empty posts
                metadata = saved_data["metadata"]
                assert metadata["subreddit"] == "empty"
                assert metadata["post_count"] == 0
                assert metadata["posts_with_media"] == 0
                assert metadata["total_media_items"] == 0

                # Check posts data is empty
                assert saved_data["posts"] == []

    @staticmethod
    def test_integration_with_unicode_content(mock_minio_client):
        """Test integration with Unicode content in posts."""
        # Posts with Unicode content
        unicode_posts = [
            {
                "id": "unicode1",
                "title": "こんにちは世界",  # Hello World in Japanese
                "author": "user_日本",
                "selftext": "This is a post with Unicode characters: 你好, привет, مرحبا"
            }
        ]

        # Save the data
        with mock.patch('bdm.ingestion.batch.reddit.storage.get_minio_client', return_value=mock_minio_client):
            # Mock the media stats function
            with mock.patch('bdm.ingestion.batch.reddit.storage.count_media_stats') as mock_count:
                mock_count.return_value = (0, 0)  # No posts with media

                save_to_storage(
                    posts=unicode_posts,
                    bucket="reddit-data",
                    subreddit="international",
                    sort_by="new",
                    time_filter="",
                    prefix="daily"
                )

                # Verify MinIO client was called
                mock_minio_client.put_object.assert_called_once()

                # Get and parse the saved data
                call_args = mock_minio_client.put_object.call_args[1]
                saved_data = json.loads(call_args["Body"])

                # Check the Unicode content was preserved
                assert saved_data["posts"][0]["title"] == "こんにちは世界"
                assert saved_data["posts"][0]["author"] == "user_日本"
                assert "你好, привет, مرحبا" in saved_data["posts"][0]["selftext"]
