"""
Integration tests for the subreddit_scraper module.
"""
import os
from unittest import mock

import pytest

from bdm.ingestion.batch.reddit.subreddit_scraper import main


class TestSubredditScraperIntegration:
    """Integration tests for the subreddit_scraper module."""

    @pytest.fixture
    def mock_env_vars(self):
        """Mock the required environment variables."""
        env_vars = {
            'REDDIT_CLIENT_ID': 'test_client_id',
            'REDDIT_CLIENT_SECRET': 'test_client_secret',
            'REDDIT_USER_AGENT': 'test_user_agent',
            'MINIO_ENDPOINT': 'http://localhost:9000',
            'MINIO_ACCESS_KEY': 'test_access_key',
            'MINIO_SECRET_KEY': 'test_secret_key'
        }
        with mock.patch.dict(os.environ, env_vars):
            yield

    @pytest.fixture
    def mock_reddit_api(self):
        """Mock the Reddit API interactions to avoid actual API calls."""
        with mock.patch('bdm.finnhub.batch.reddit.reddit_api.get_reddit_client') as mock_client:
            # Configure the mock client
            mock_reddit = mock.MagicMock()
            mock_subreddit = mock.MagicMock()
            mock_post1 = mock.MagicMock()
            mock_post2 = mock.MagicMock()

            # Set up post attributes
            for idx, post in enumerate([mock_post1, mock_post2], 1):
                post.id = f'test_id_{idx}'
                post.title = f'Test Post {idx}'
                post.author = 'test_author'
                post.created_utc = 1234567890 + idx
                post.score = 100 + idx
                post.url = f'https://example.com/post{idx}'
                post.selftext = f'Test content {idx}'
                post.num_comments = 10 + idx
                post.permalink = f'/r/test/comments/test_id_{idx}/test_title_{idx}/'
                post.upvote_ratio = 0.95
                post.is_original_content = False
                post.is_self = False

            # Chain the mocks
            mock_reddit.subreddit.return_value = mock_subreddit
            mock_subreddit.hot.return_value = [mock_post1, mock_post2]
            mock_subreddit.new.return_value = [mock_post1, mock_post2]
            mock_subreddit.top.return_value = [mock_post1, mock_post2]
            mock_subreddit.rising.return_value = [mock_post1, mock_post2]
            mock_subreddit.controversial.return_value = [mock_post1, mock_post2]

            mock_client.return_value = mock_reddit
            yield mock_client

    @pytest.fixture
    def mock_media_download(self):
        """Mock media downloads to avoid actual HTTP requests."""
        with mock.patch('bdm.finnhub.batch.reddit.media_utils.download_media') as mock_download:
            # Return None to simulate no media downloaded
            mock_download.return_value = None
            yield mock_download

    @pytest.fixture
    def mock_boto3_client(self):
        """Mock boto3 client to prevent any actual AWS API calls."""
        with mock.patch('boto3.client') as mock_boto3:
            mock_s3_client = mock.MagicMock()
            mock_boto3.return_value = mock_s3_client
            yield mock_boto3

    @staticmethod
    def test_end_to_end_flow(mock_env_vars, mock_reddit_api, mock_media_download, mock_boto3_client):
        """Test the end-to-end flow with mocked external dependencies."""
        # Run the main function
        main(
            subreddit='integration_test',
            limit=10,
            sort_by='hot',
            time_filter='day',
            bucket='test-bucket',
            prefix='integration',
            skip_media=False
        )

        # Verify Reddit API was called correctly
        mock_reddit_api.return_value.subreddit.assert_called_once_with('integration_test')
        mock_reddit_api.return_value.subreddit.return_value.hot.assert_called_once()

        # Verify boto3 client was called with the right parameters
        mock_boto3_client.assert_called_once_with(
            's3',
            endpoint_url='http://localhost:9000',
            aws_access_key_id='test_access_key',
            aws_secret_access_key='test_secret_key',
            config=mock.ANY,
            region_name='us-east-1'
        )

        # Verify put_object was called
        assert mock_boto3_client.return_value.put_object.call_count == 1

        # Get the put_object call args
        call_args = mock_boto3_client.return_value.put_object.call_args
        bucket_arg = call_args[1]['Bucket']
        key_arg = call_args[1]['Key']
        content_type_arg = call_args[1]['ContentType']

        # Verify S3 arguments
        assert bucket_arg == 'test-bucket'
        assert 'reddit/integration/integration_test/' in key_arg
        assert content_type_arg == 'application/json'

    @staticmethod
    def test_end_to_end_with_skip_media(mock_env_vars, mock_reddit_api, mock_media_download, mock_boto3_client):
        """Test the end-to-end flow with skip_media=True."""
        # Run the main function with skip_media=True
        main(
            subreddit='integration_test',
            limit=5,
            sort_by='new',
            time_filter='all',
            bucket='test-bucket',
            prefix='integration',
            skip_media=True
        )

        # Verify Reddit API was called correctly
        mock_reddit_api.return_value.subreddit.assert_called_once_with('integration_test')
        mock_reddit_api.return_value.subreddit.return_value.new.assert_called_once()

        # Verify boto3 client was called
        assert mock_boto3_client.call_count >= 1

        # Verify put_object was called
        assert mock_boto3_client.return_value.put_object.call_count == 1

        # Verify media download was not attempted due to skip_media flag
        assert mock_media_download.call_count == 0

    @staticmethod
    def test_different_sort_methods(mock_env_vars, mock_reddit_api, mock_boto3_client):
        """Test all different sort methods."""
        sort_methods = [
            ('new', 'all'),
            ('hot', 'all'),
            ('rising', 'all'),
            ('top', 'week'),
            ('controversial', 'month')
        ]

        for sort_by, time_filter in sort_methods:
            # Reset the mocks
            mock_reddit_api.reset_mock()
            mock_boto3_client.reset_mock()
            mock_boto3_client.return_value.reset_mock()

            # Run the main function with this sort method
            main(
                subreddit='integration_test',
                limit=2,
                sort_by=sort_by,
                time_filter=time_filter,
                bucket='test-bucket',
                prefix=f'integration/{sort_by}',
                skip_media=True
            )

            # Verify the correct sort method was called
            getattr(mock_reddit_api.return_value.subreddit.return_value, sort_by).assert_called_once()

            # Verify boto3 client was called
            assert mock_boto3_client.call_count >= 1

            # Verify put_object was called
            assert mock_boto3_client.return_value.put_object.call_count == 1
