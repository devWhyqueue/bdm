"""
Unit tests for Reddit API scraper functionality.
"""
from unittest import mock

import pytest
from praw.reddit import Submission, Subreddit

from bdm.ingestion.batch.reddit.reddit_api import scrape_subreddit


class TestSubredditScraper:
    """Tests for the subreddit scraper function."""

    @pytest.fixture
    def mock_reddit_client(self):
        """Create a mock Reddit client."""
        with mock.patch('bdm.finnhub.batch.reddit.reddit_api.get_reddit_client') as mock_client:
            # Configure the mock client
            mock_client.return_value = mock.MagicMock()
            yield mock_client.return_value

    @pytest.fixture
    def mock_subreddit(self):
        """Create a mock subreddit."""
        mock_sub = mock.MagicMock(spec=Subreddit)
        return mock_sub

    @pytest.fixture
    def mock_post(self):
        """Create a mock post."""
        mock_post = mock.MagicMock(spec=Submission)
        mock_post.id = 'test_id'
        mock_post.title = 'Test Title'
        mock_post.author = 'test_author'
        mock_post.created_utc = 1234567890
        mock_post.score = 100
        mock_post.url = 'https://example.com'
        mock_post.selftext = 'Test content'
        mock_post.num_comments = 10
        mock_post.permalink = '/r/test/comments/test_id/test_title/'
        mock_post.upvote_ratio = 0.95
        mock_post.is_original_content = False
        mock_post.is_self = False
        return mock_post

    @pytest.fixture
    def mock_posts_generator(self):
        """Create a mock posts generator."""
        with mock.patch('bdm.finnhub.batch.reddit.reddit_api.get_posts_generator') as mock_generator:
            yield mock_generator

    @pytest.fixture
    def mock_extract_post_data(self):
        """Create a mock for extract_post_data function."""
        with mock.patch('bdm.finnhub.batch.reddit.reddit_api.extract_post_data') as mock_extract:
            mock_extract.return_value = {'id': 'test_id', 'title': 'Test Title'}
            yield mock_extract

    @staticmethod
    def test_scrape_subreddit_success(mock_reddit_client, mock_posts_generator,
                                      mock_subreddit, mock_post, mock_extract_post_data):
        """Test successful scraping of a subreddit."""
        # Configure the mocks
        mock_reddit_client.subreddit.return_value = mock_subreddit
        mock_posts_generator.return_value = [mock_post, mock_post]  # Return 2 posts

        # Call the function
        result = scrape_subreddit(
            subreddit_name='test',
            sort_by='hot',
            time_filter='day',
            limit=10,
            bucket='test-bucket',
            prefix='test-prefix'
        )

        # Assert
        mock_reddit_client.subreddit.assert_called_once_with('test')
        mock_posts_generator.assert_called_once_with(mock_subreddit, 'hot', 'day', 10)
        assert mock_extract_post_data.call_count == 2
        assert len(result) == 2
        for post_data in result:
            assert post_data == {'id': 'test_id', 'title': 'Test Title'}

    @staticmethod
    def test_scrape_subreddit_no_posts(mock_reddit_client, mock_posts_generator,
                                       mock_subreddit, mock_extract_post_data):
        """Test scraping a subreddit with no posts."""
        # Configure the mocks
        mock_reddit_client.subreddit.return_value = mock_subreddit
        mock_posts_generator.return_value = []  # Return no posts

        # Call the function
        result = scrape_subreddit(
            subreddit_name='test',
            sort_by='new',
            time_filter='all',
            limit=5,
            bucket='test-bucket',
            prefix='test-prefix'
        )

        # Assert
        mock_reddit_client.subreddit.assert_called_once_with('test')
        mock_posts_generator.assert_called_once_with(mock_subreddit, 'new', 'all', 5)
        assert mock_extract_post_data.call_count == 0
        assert len(result) == 0

    @staticmethod
    def test_scrape_subreddit_client_error(mock_reddit_client):
        """Test handling of Reddit client error."""
        # Configure the mock to raise an exception
        mock_reddit_client.subreddit.side_effect = Exception("API error")

        # Assert that the error is propagated
        with pytest.raises(Exception) as excinfo:
            scrape_subreddit(
                subreddit_name='test',
                sort_by='top',
                time_filter='month',
                limit=25,
                bucket='test-bucket',
                prefix='test-prefix'
            )

        assert "API error" in str(excinfo.value)

    @staticmethod
    def test_scrape_subreddit_extraction_error(mock_reddit_client, mock_posts_generator,
                                               mock_subreddit, mock_post):
        """Test handling of post extraction error."""
        # Configure the mocks
        mock_reddit_client.subreddit.return_value = mock_subreddit
        mock_posts_generator.return_value = [mock_post]

        # Configure extract_post_data to raise an exception
        with mock.patch('bdm.ingestion.batch.reddit.reddit_api.extract_post_data') as mock_extract:
            mock_extract.side_effect = Exception("Extraction error")

            # Assert that the error is propagated
            with pytest.raises(Exception) as excinfo:
                scrape_subreddit(
                    subreddit_name='test',
                    sort_by='controversial',
                    time_filter='year',
                    limit=100,
                    bucket='test-bucket',
                    prefix='test-prefix'
                )

            assert "Extraction error" in str(excinfo.value)
