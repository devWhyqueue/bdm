from bdm.ingestion.batch.reddit.media_utils import (
    get_file_extension,
    get_content_type
)


class TestFileHandling:
    """Tests for file handling utilities."""

    @staticmethod
    def test_get_file_extension_from_url():
        """Test extracting file extension from URL."""
        url = "https://example.com/image.jpg"
        extension = get_file_extension(url, "image")

        assert extension == ".jpg"

    @staticmethod
    def test_get_file_extension_default_image():
        """Test default extension for image type."""
        url = "https://example.com/image"
        extension = get_file_extension(url, "image")

        assert extension == ".jpg"

    @staticmethod
    def test_get_file_extension_default_gif():
        """Test default extension for gif type."""
        url = "https://example.com/image"
        extension = get_file_extension(url, "gif")

        assert extension == ".gif"

    @staticmethod
    def test_get_file_extension_default_video():
        """Test default extension for video type."""
        url = "https://example.com/video"
        extension = get_file_extension(url, "video")

        assert extension == ".mp4"

    @staticmethod
    def test_get_file_extension_default_unknown():
        """Test default extension for unknown type."""
        url = "https://example.com/file"
        extension = get_file_extension(url, "unknown")

        assert extension == ".jpg"  # Default fallback

    @staticmethod
    def test_get_content_type_png():
        """Test content type for PNG."""
        content_type = get_content_type(".png")
        assert content_type == "image/png"

    @staticmethod
    def test_get_content_type_gif():
        """Test content type for GIF."""
        content_type = get_content_type(".gif")
        assert content_type == "image/gif"

    @staticmethod
    def test_get_content_type_webp():
        """Test content type for WEBP."""
        content_type = get_content_type(".webp")
        assert content_type == "image/webp"

    @staticmethod
    def test_get_content_type_mp4():
        """Test content type for MP4."""
        content_type = get_content_type(".mp4")
        assert content_type == "video/mp4"

    @staticmethod
    def test_get_content_type_webm():
        """Test content type for WEBM."""
        content_type = get_content_type(".webm")
        assert content_type == "video/webm"

    @staticmethod
    def test_get_content_type_default():
        """Test default content type."""
        content_type = get_content_type(".unknown")
        assert content_type == "image/jpeg"
