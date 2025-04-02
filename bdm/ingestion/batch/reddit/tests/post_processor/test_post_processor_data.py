import math
from unittest import mock

from praw.reddit import Submission

from bdm.ingestion.batch.reddit.post_processor import (
    extract_basic_post_data,
    extract_post_data,
    count_media_stats
)


class TestExtractBasicPostData:
    """Tests for the extract_basic_post_data function."""

    @staticmethod
    def test_extract_basic_post_data():
        """Test extraction of basic post data."""
        # Create a mock post
        mock_post = mock.Mock(spec=Submission)
        mock_post.id = "post123"
        mock_post.title = "Test Post"
        mock_post.author = "testuser"
        mock_post.created_utc = 1617235200  # Example timestamp
        mock_post.score = 100
        mock_post.url = "https://example.com/post123"
        mock_post.selftext = "This is a test post."
        mock_post.num_comments = 42
        mock_post.permalink = "/r/testsubreddit/comments/post123/test_post/"
        mock_post.upvote_ratio = 0.95
        mock_post.is_original_content = True
        mock_post.is_self = False

        # Call function
        result = extract_basic_post_data(mock_post)

        # Check results
        assert result["id"] == "post123"
        assert result["title"] == "Test Post"
        assert result["author"] == "testuser"
        assert result["created_utc"] == 1617235200
        assert result["score"] == 100
        assert result["url"] == "https://example.com/post123"
        assert result["selftext"] == "This is a test post."
        assert result["num_comments"] == 42
        assert result["permalink"] == "/r/testsubreddit/comments/post123/test_post/"
        assert math.isclose(result["upvote_ratio"], 0.95)
        assert result["is_original_content"] is True
        assert result["is_self"] is False


class TestExtractPostData:
    """Tests for the extract_post_data function."""

    @mock.patch('bdm.ingestion.batch.reddit.post_processor.is_media_url')
    @mock.patch('bdm.ingestion.batch.reddit.post_processor.process_single_media')
    @mock.patch('bdm.ingestion.batch.reddit.post_processor.process_gallery_post')
    def test_extract_post_data_with_direct_media(self, mock_process_gallery,
                                                 mock_process_media, mock_is_media_url):
        """Test extraction of post data with direct media."""
        # Create a mock post
        mock_post = mock.Mock(spec=Submission)
        mock_post.id = "post123"
        mock_post.title = "Test Post"
        mock_post.author = "testuser"
        mock_post.created_utc = 1617235200
        mock_post.score = 100
        mock_post.url = "https://example.com/image.jpg"
        mock_post.selftext = ""
        mock_post.num_comments = 42
        mock_post.permalink = "/r/testsubreddit/comments/post123/test_post/"
        mock_post.upvote_ratio = 0.95
        mock_post.is_original_content = False
        mock_post.is_self = False
        mock_post.is_gallery = False

        # Configure mocks
        mock_is_media_url.return_value = (True, "image")
        mock_process_media.return_value = {
            "media_type": "image",
            "s3_url": "reddit/test/subreddit/images/image.jpg",
            "source_url": "https://example.com/image.jpg",
            "filename": "image.jpg",
            "content_type": "image/jpeg"
        }
        mock_process_gallery.return_value = []

        # Call function
        result = extract_post_data(
            post=mock_post,
            bucket="test-bucket",
            subreddit="testsubreddit",
            prefix="test"
        )

        # Check results
        assert result["id"] == "post123"
        assert "media" in result
        assert len(result["media"]) == 1
        assert result["media"][0]["media_type"] == "image"

        # Verify calls
        mock_is_media_url.assert_called_once_with("https://example.com/image.jpg")
        mock_process_media.assert_called_once_with(
            mock_post, "image", "test-bucket", "testsubreddit", "test"
        )
        mock_process_gallery.assert_called_once_with(
            mock_post, "test-bucket", "testsubreddit", "test"
        )

    @mock.patch('bdm.ingestion.batch.reddit.post_processor.is_media_url')
    @mock.patch('bdm.ingestion.batch.reddit.post_processor.process_gallery_post')
    def test_extract_post_data_with_gallery(self, mock_process_gallery, mock_is_media_url):
        """Test extraction of post data with gallery."""
        # Create a mock gallery post
        mock_post = mock.Mock(spec=Submission)
        mock_post.id = "post123"
        mock_post.title = "Test Gallery"
        mock_post.author = "testuser"
        mock_post.created_utc = 1617235200
        mock_post.score = 100
        mock_post.url = "https://www.reddit.com/gallery/post123"
        mock_post.selftext = ""
        mock_post.num_comments = 42
        mock_post.permalink = "/r/testsubreddit/comments/post123/test_gallery/"
        mock_post.upvote_ratio = 0.95
        mock_post.is_original_content = False
        mock_post.is_self = False
        mock_post.is_gallery = True

        # Configure mocks
        mock_is_media_url.return_value = (False, None)
        mock_process_gallery.return_value = [
            {
                "media_type": "image",
                "s3_url": "reddit/test/subreddit/images/image1.jpg",
                "source_url": "https://example.com/gallery/image1.jpg",
                "filename": "image1.jpg",
                "content_type": "image/jpeg",
                "gallery_item_id": "media1"
            },
            {
                "media_type": "image",
                "s3_url": "reddit/test/subreddit/images/image2.jpg",
                "source_url": "https://example.com/gallery/image2.jpg",
                "filename": "image2.jpg",
                "content_type": "image/jpeg",
                "gallery_item_id": "media2"
            }
        ]

        # Call function
        result = extract_post_data(
            post=mock_post,
            bucket="test-bucket",
            subreddit="testsubreddit",
            prefix="test"
        )

        # Check results
        assert result["id"] == "post123"
        assert "media" in result
        assert len(result["media"]) == 2
        assert result["media"][0]["gallery_item_id"] == "media1"
        assert result["media"][1]["gallery_item_id"] == "media2"

    @mock.patch('bdm.ingestion.batch.reddit.post_processor.is_media_url')
    @mock.patch('bdm.ingestion.batch.reddit.post_processor.process_gallery_post')
    def test_extract_post_data_no_media(self, mock_process_gallery, mock_is_media_url):
        """Test extraction of post data without media."""
        # Create a mock text post
        mock_post = mock.Mock(spec=Submission)
        mock_post.id = "post123"
        mock_post.title = "Test Text Post"
        mock_post.author = "testuser"
        mock_post.created_utc = 1617235200
        mock_post.score = 100
        mock_post.url = "https://www.reddit.com/r/testsubreddit/comments/post123/test_text_post/"
        mock_post.selftext = "This is a test text post with no media."
        mock_post.num_comments = 42
        mock_post.permalink = "/r/testsubreddit/comments/post123/test_text_post/"
        mock_post.upvote_ratio = 0.95
        mock_post.is_original_content = False
        mock_post.is_self = True
        mock_post.is_gallery = False

        # Configure mocks
        mock_is_media_url.return_value = (False, None)
        mock_process_gallery.return_value = []

        # Call function
        result = extract_post_data(
            post=mock_post,
            bucket="test-bucket",
            subreddit="testsubreddit",
            prefix="test"
        )

        # Check results
        assert result["id"] == "post123"
        assert "media" not in result


class TestCountMediaStats:
    """Tests for the count_media_stats function."""

    @staticmethod
    def test_count_media_stats():
        """Test counting media statistics."""
        # Create test data
        posts = [
            {
                "id": "post1",
                "title": "Post with one media item",
                "media": [
                    {"media_type": "image", "s3_url": "path/to/image.jpg"}
                ]
            },
            {
                "id": "post2",
                "title": "Post with two media items",
                "media": [
                    {"media_type": "image", "s3_url": "path/to/image1.jpg"},
                    {"media_type": "image", "s3_url": "path/to/image2.jpg"}
                ]
            },
            {
                "id": "post3",
                "title": "Post with no media"
            },
            {
                "id": "post4",
                "title": "Another post with no media"
            },
            {
                "id": "post5",
                "title": "Post with three media items",
                "media": [
                    {"media_type": "image", "s3_url": "path/to/image3.jpg"},
                    {"media_type": "video", "s3_url": "path/to/video.mp4"},
                    {"media_type": "gif", "s3_url": "path/to/animation.gif"}
                ]
            }
        ]

        # Call function
        posts_with_media, total_media_items = count_media_stats(posts)

        # Check results
        assert posts_with_media == 3  # posts 1, 2, and 5
        assert total_media_items == 6  # 1 + 2 + 3

    @staticmethod
    def test_count_media_stats_empty_list():
        """Test counting media statistics with empty list."""
        # Call function with empty list
        posts_with_media, total_media_items = count_media_stats([])

        # Check results
        assert posts_with_media == 0
        assert total_media_items == 0

    @staticmethod
    def test_count_media_stats_no_media():
        """Test counting media statistics when no posts have media."""
        # Create test data without media
        posts = [
            {"id": "post1", "title": "Post without media"},
            {"id": "post2", "title": "Another post without media"},
            {"id": "post3", "title": "Yet another post without media"}
        ]

        # Call function
        posts_with_media, total_media_items = count_media_stats(posts)

        # Check results
        assert posts_with_media == 0
        assert total_media_items == 0
