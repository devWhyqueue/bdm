from unittest import mock

from praw.reddit import Submission

from bdm.ingestion.batch.reddit.post_processor import (
    create_media_item,
    process_single_media,
    process_gallery_image
)


class TestCreateMediaItem:
    """Tests for the create_media_item function."""

    @staticmethod
    def test_create_media_item_basic():
        """Test creation of basic media item."""
        media_item = create_media_item(
            media_type="image",
            s3_key="reddit/test/subreddit/images/image.jpg",
            url="https://example.com/image.jpg",
            filename="image.jpg",
            content_type="image/jpeg"
        )

        assert media_item["media_type"] == "image"
        assert media_item["s3_url"] == "reddit/test/subreddit/images/image.jpg"
        assert media_item["source_url"] == "https://example.com/image.jpg"
        assert media_item["filename"] == "image.jpg"
        assert media_item["content_type"] == "image/jpeg"
        assert "gallery_item_id" not in media_item

    @staticmethod
    def test_create_media_item_with_gallery_id():
        """Test creation of gallery media item with ID."""
        media_item = create_media_item(
            media_type="image",
            s3_key="reddit/test/subreddit/images/image.jpg",
            url="https://example.com/image.jpg",
            filename="image.jpg",
            content_type="image/jpeg",
            gallery_item_id="gallery123"
        )

        assert media_item["media_type"] == "image"
        assert media_item["s3_url"] == "reddit/test/subreddit/images/image.jpg"
        assert media_item["source_url"] == "https://example.com/image.jpg"
        assert media_item["filename"] == "image.jpg"
        assert media_item["content_type"] == "image/jpeg"
        assert media_item["gallery_item_id"] == "gallery123"


class TestProcessSingleMedia:
    """Tests for the process_single_media function."""

    @mock.patch('bdm.ingestion.batch.reddit.post_processor.download_media')
    @mock.patch('bdm.ingestion.batch.reddit.post_processor.upload_media_to_s3')
    def test_process_single_media_success(self, mock_upload, mock_download):
        """Test successful reddit of single media."""
        # Create a mock post
        mock_post = mock.Mock(spec=Submission)
        mock_post.url = "https://example.com/image.jpg"

        # Configure mocks
        mock_download.return_value = ("image.jpg", "image/jpeg", b"content")
        mock_upload.return_value = "reddit/test/subreddit/images/image.jpg"

        # Call function
        result = process_single_media(
            post=mock_post,
            media_type="image",
            bucket="test-bucket",
            subreddit="testsubreddit",
            prefix="test"
        )

        # Check results
        assert result is not None
        assert result["media_type"] == "image"
        assert result["s3_url"] == "reddit/test/subreddit/images/image.jpg"
        assert result["source_url"] == "https://example.com/image.jpg"
        assert result["filename"] == "image.jpg"
        assert result["content_type"] == "image/jpeg"

        # Verify calls
        mock_download.assert_called_once_with("https://example.com/image.jpg", "image")
        mock_upload.assert_called_once_with(
            "test-bucket", "testsubreddit", "test",
            "image", "image.jpg", b"content", "image/jpeg"
        )

    @mock.patch('bdm.ingestion.batch.reddit.post_processor.download_media')
    def test_process_single_media_download_failure(self, mock_download):
        """Test handling of download failure."""
        # Create a mock post
        mock_post = mock.Mock(spec=Submission)
        mock_post.url = "https://example.com/image.jpg"

        # Configure mocks to simulate download failure
        mock_download.return_value = None

        # Call function
        result = process_single_media(
            post=mock_post,
            media_type="image",
            bucket="test-bucket",
            subreddit="testsubreddit",
            prefix="test"
        )

        # Check results
        assert result is None

    @mock.patch('bdm.ingestion.batch.reddit.post_processor.download_media')
    @mock.patch('bdm.ingestion.batch.reddit.post_processor.upload_media_to_s3')
    def test_process_single_media_upload_failure(self, mock_upload, mock_download):
        """Test handling of upload failure."""
        # Create a mock post
        mock_post = mock.Mock(spec=Submission)
        mock_post.url = "https://example.com/image.jpg"

        # Configure mocks
        mock_download.return_value = ("image.jpg", "image/jpeg", b"content")
        mock_upload.return_value = None  # Simulate upload failure

        # Call function
        result = process_single_media(
            post=mock_post,
            media_type="image",
            bucket="test-bucket",
            subreddit="testsubreddit",
            prefix="test"
        )

        # Check results
        assert result is None


class TestProcessGalleryImage:
    """Tests for the process_gallery_image function."""

    @mock.patch('bdm.ingestion.batch.reddit.post_processor.download_media')
    @mock.patch('bdm.ingestion.batch.reddit.post_processor.upload_media_to_s3')
    def test_process_gallery_image_success(self, mock_upload, mock_download):
        """Test successful reddit of gallery image."""
        # Configure mocks
        mock_download.return_value = ("image.jpg", "image/jpeg", b"content")
        mock_upload.return_value = "reddit/test/subreddit/images/image.jpg"

        # Call function
        result = process_gallery_image(
            image_url="https://example.com/gallery/image.jpg",
            media_id="media123",
            bucket="test-bucket",
            subreddit="testsubreddit",
            prefix="test"
        )

        # Check results
        assert result is not None
        assert result["media_type"] == "image"
        assert result["s3_url"] == "reddit/test/subreddit/images/image.jpg"
        assert result["source_url"] == "https://example.com/gallery/image.jpg"
        assert result["filename"] == "image.jpg"
        assert result["content_type"] == "image/jpeg"
        assert result["gallery_item_id"] == "media123"

    @mock.patch('bdm.ingestion.batch.reddit.post_processor.download_media')
    def test_process_gallery_image_download_failure(self, mock_download):
        """Test handling of download failure for gallery image."""
        # Configure mocks
        mock_download.return_value = None

        # Call function
        result = process_gallery_image(
            image_url="https://example.com/gallery/image.jpg",
            media_id="media123",
            bucket="test-bucket",
            subreddit="testsubreddit",
            prefix="test"
        )

        # Check results
        assert result is None

    @mock.patch('bdm.ingestion.batch.reddit.post_processor.download_media')
    @mock.patch('bdm.ingestion.batch.reddit.post_processor.upload_media_to_s3')
    def test_process_gallery_image_upload_failure(self, mock_upload, mock_download):
        """Test handling of upload failure for gallery image."""
        # Configure mocks
        mock_download.return_value = ("image.jpg", "image/jpeg", b"content")
        mock_upload.return_value = None  # Simulate upload failure

        # Call function
        result = process_gallery_image(
            image_url="https://example.com/gallery/image.jpg",
            media_id="media123",
            bucket="test-bucket",
            subreddit="testsubreddit",
            prefix="test"
        )

        # Check results
        assert result is None
