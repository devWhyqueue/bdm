from unittest import mock

from praw.reddit import Submission

from bdm.ingestion.batch.reddit.post_processor import process_gallery_post


class TestProcessGalleryPost:
    """Tests for the process_gallery_post function."""

    @mock.patch('bdm.ingestion.batch.reddit.post_processor.process_gallery_image')
    def test_process_gallery_post_success(self, mock_process_image):
        """Test successful processing of gallery post."""
        # Create a mock gallery post
        mock_post = mock.Mock(spec=Submission)
        mock_post.is_gallery = True
        mock_post.media_metadata = {
            'media1': {
                'e': 'Image',
                's': {'u': 'https://example.com/gallery/image1.jpg'}
            },
            'media2': {
                'e': 'Image',
                's': {'u': 'https://example.com/gallery/image2.jpg'}
            }
        }

        # Configure mock
        mock_process_image.side_effect = [
            {
                'media_type': 'image',
                's3_url': 'reddit/test/subreddit/images/image1.jpg',
                'source_url': 'https://example.com/gallery/image1.jpg',
                'filename': 'image1.jpg',
                'content_type': 'image/jpeg',
                'gallery_item_id': 'media1'
            },
            {
                'media_type': 'image',
                's3_url': 'reddit/test/subreddit/images/image2.jpg',
                'source_url': 'https://example.com/gallery/image2.jpg',
                'filename': 'image2.jpg',
                'content_type': 'image/jpeg',
                'gallery_item_id': 'media2'
            }
        ]

        # Call function
        result = process_gallery_post(
            post=mock_post,
            bucket="test-bucket",
            subreddit="testsubreddit",
            prefix="test"
        )

        # Check results
        assert len(result) == 2
        assert result[0]['gallery_item_id'] == 'media1'
        assert result[1]['gallery_item_id'] == 'media2'

        # Verify calls
        assert mock_process_image.call_count == 2
        mock_process_image.assert_any_call(
            'https://example.com/gallery/image1.jpg',
            'media1',
            'test-bucket',
            'testsubreddit',
            'test'
        )
        mock_process_image.assert_any_call(
            'https://example.com/gallery/image2.jpg',
            'media2',
            'test-bucket',
            'testsubreddit',
            'test'
        )

    @staticmethod
    def test_process_gallery_post_not_gallery():
        """Test handling of non-gallery posts."""
        # Create a mock non-gallery post
        mock_post = mock.Mock(spec=Submission)
        mock_post.is_gallery = False

        # Call function
        result = process_gallery_post(
            post=mock_post,
            bucket="test-bucket",
            subreddit="testsubreddit",
            prefix="test"
        )

        # Check results
        assert result == []

    @staticmethod
    def test_process_gallery_post_invalid_metadata():
        """Test handling of gallery posts with invalid metadata."""
        # Create a mock gallery post with invalid metadata
        mock_post = mock.Mock(spec=Submission)
        mock_post.is_gallery = True
        mock_post.media_metadata = {
            'media1': {
                'e': 'Video',  # Not an image
                's': {'u': 'https://example.com/gallery/video.mp4'}
            },
            'media2': {
                'e': 'Image',
                # Missing 's' field
            },
            'media3': {
                'e': 'Image',
                's': {}  # Missing 'u' field
            }
        }

        # Call function
        result = process_gallery_post(
            post=mock_post,
            bucket="test-bucket",
            subreddit="testsubreddit",
            prefix="test"
        )

        # Check results
        assert result == []

    @mock.patch('bdm.ingestion.batch.reddit.post_processor.process_gallery_image')
    def test_process_gallery_post_with_failed_image(self, mock_process_image):
        """Test handling when some gallery images fail to process."""
        # Create a mock gallery post
        mock_post = mock.Mock(spec=Submission)
        mock_post.is_gallery = True
        mock_post.media_metadata = {
            'media1': {
                'e': 'Image',
                's': {'u': 'https://example.com/gallery/image1.jpg'}
            },
            'media2': {
                'e': 'Image',
                's': {'u': 'https://example.com/gallery/image2.jpg'}
            }
        }

        # Configure mock to have one success and one failure
        mock_process_image.side_effect = [
            {
                'media_type': 'image',
                's3_url': 'reddit/test/subreddit/images/image1.jpg',
                'source_url': 'https://example.com/gallery/image1.jpg',
                'filename': 'image1.jpg',
                'content_type': 'image/jpeg',
                'gallery_item_id': 'media1'
            },
            None  # Second image fails
        ]

        # Call function
        result = process_gallery_post(
            post=mock_post,
            bucket="test-bucket",
            subreddit="testsubreddit",
            prefix="test"
        )

        # Check results
        assert len(result) == 1
        assert result[0]['gallery_item_id'] == 'media1'

    @mock.patch('bdm.ingestion.batch.reddit.post_processor.process_gallery_image')
    def test_process_gallery_post_exception(self, mock_process_image):
        """Test handling of exceptions during gallery processing."""
        # Create a mock gallery post
        mock_post = mock.Mock(spec=Submission)
        mock_post.is_gallery = True
        mock_post.id = "post123"
        mock_post.media_metadata = {
            'media1': {
                'e': 'Image',
                's': {'u': 'https://example.com/gallery/image1.jpg'}
            }
        }

        # Configure mock to raise exception
        mock_process_image.side_effect = Exception("Test exception")

        # Call function and verify it handles the exception without raising
        result = process_gallery_post(
            post=mock_post,
            bucket="test-bucket",
            subreddit="testsubreddit",
            prefix="test"
        )

        # Check results
        assert result == []
