"""
Unit tests for Reddit API client functionality.
"""
import os
from unittest import mock

import pytest

from bdm.ingestion.batch.reddit.reddit_api import (
    get_reddit_client,
    get_posts_generator
)


class TestRedditClient:
    """Tests for the Reddit client initialization."""

    @staticmethod
    def test_get_reddit_client_success():
        """Test successful Reddit client initialization."""
        # Mock environment variables
        with mock.patch.dict(os.environ, {
            'REDDIT_CLIENT_ID': 'test_id',
            'REDDIT_CLIENT_SECRET': 'test_secret',
            'REDDIT_USER_AGENT': 'test_agent'
        }), mock.patch('praw.Reddit') as mock_reddit:
            # Configure the mock
            mock_reddit.return_value = mock.MagicMock()

            # Call the function
            client = get_reddit_client()

            # Assert
            mock_reddit.assert_called_once_with(
                client_id='test_id',
                client_secret='test_secret',
                user_agent='test_agent'
            )
            assert client == mock_reddit.return_value

    @staticmethod
    def test_get_reddit_client_missing_credentials():
        """Test Reddit client initialization with missing credentials."""
        # Mock environment variables with missing credentials
        with mock.patch.dict(os.environ, {}, clear=True):
            # Assert that an error is raised
            with pytest.raises(ValueError) as excinfo:
                get_reddit_client()

            assert "Reddit API credentials not found" in str(excinfo.value)

    @staticmethod
    def test_get_reddit_client_partial_credentials():
        """Test Reddit client initialization with partial credentials."""
        # Mock environment variables with only client_id
        with mock.patch.dict(os.environ, {
            'REDDIT_CLIENT_ID': 'test_id',
            'REDDIT_USER_AGENT': 'test_agent'
        }, clear=True):
            # Assert that an error is raised
            with pytest.raises(ValueError) as excinfo:
                get_reddit_client()

            assert "Reddit API credentials not found" in str(excinfo.value)


class TestPostsGenerator:
    """Tests for the posts generator function."""

    @pytest.fixture
    def mock_subreddit(self):
        """Create a mock subreddit object."""
        mock_sub = mock.MagicMock()
        # Setup methods on the subreddit
        mock_sub.new = mock.MagicMock(return_value=["post1", "post2"])
        mock_sub.hot = mock.MagicMock(return_value=["post1", "post2"])
        mock_sub.rising = mock.MagicMock(return_value=["post1", "post2"])
        mock_sub.top = mock.MagicMock(return_value=["post1", "post2"])
        mock_sub.controversial = mock.MagicMock(return_value=["post1", "post2"])
        return mock_sub

    @staticmethod
    def test_get_posts_generator_new(mock_subreddit):
        """Test getting posts with 'new' sort method."""
        # Call the function
        posts = get_posts_generator(mock_subreddit, 'new', 'all', 10)

        # Assert
        mock_subreddit.new.assert_called_once_with(limit=10)
        assert posts == ["post1", "post2"]

    @staticmethod
    def test_get_posts_generator_hot(mock_subreddit):
        """Test getting posts with 'hot' sort method."""
        # Call the function
        posts = get_posts_generator(mock_subreddit, 'hot', 'all', 10)

        # Assert
        mock_subreddit.hot.assert_called_once_with(limit=10)
        assert posts == ["post1", "post2"]

    @staticmethod
    def test_get_posts_generator_rising(mock_subreddit):
        """Test getting posts with 'rising' sort method."""
        # Call the function
        posts = get_posts_generator(mock_subreddit, 'rising', 'all', 10)

        # Assert
        mock_subreddit.rising.assert_called_once_with(limit=10)
        assert posts == ["post1", "post2"]

    @staticmethod
    def test_get_posts_generator_top(mock_subreddit):
        """Test getting posts with 'top' sort method."""
        # Call the function
        posts = get_posts_generator(mock_subreddit, 'top', 'day', 10)

        # Assert
        mock_subreddit.top.assert_called_once_with(time_filter='day', limit=10)
        assert posts == ["post1", "post2"]

    @staticmethod
    def test_get_posts_generator_controversial(mock_subreddit):
        """Test getting posts with 'controversial' sort method."""
        # Call the function
        posts = get_posts_generator(mock_subreddit, 'controversial', 'week', 10)

        # Assert
        mock_subreddit.controversial.assert_called_once_with(time_filter='week', limit=10)
        assert posts == ["post1", "post2"]

    @staticmethod
    def test_get_posts_generator_invalid_sort(mock_subreddit):
        """Test getting posts with invalid sort method."""
        # Assert that an error is raised
        with pytest.raises(ValueError) as excinfo:
            get_posts_generator(mock_subreddit, 'invalid', 'all', 10)

        assert "Invalid sort_by value: invalid" in str(excinfo.value)
