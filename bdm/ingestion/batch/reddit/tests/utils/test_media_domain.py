from urllib.parse import urlparse

from bdm.ingestion.batch.reddit.media_utils import (
    check_known_media_domain,
    is_media_url
)


class TestMediaDomainDetection:
    """Tests for media domain detection functions."""

    @staticmethod
    def test_check_known_media_domain_image():
        """Test image detection from known domains."""
        url = "https://i.redd.it/example.jpg"
        parsed_url = urlparse(url)
        is_media, media_type = check_known_media_domain(parsed_url)

        assert is_media is True
        assert media_type == "image"

    @staticmethod
    def test_check_known_media_domain_gif():
        """Test gif detection from known domains."""
        url = "https://i.redd.it/example.gif"
        parsed_url = urlparse(url)
        is_media, media_type = check_known_media_domain(parsed_url)

        assert is_media is True
        assert media_type == "gif"

    @staticmethod
    def test_check_known_media_domain_video():
        """Test video detection from known domains."""
        url = "https://v.redd.it/example"
        parsed_url = urlparse(url)
        is_media, media_type = check_known_media_domain(parsed_url)

        assert is_media is True
        assert media_type == "video"

    @staticmethod
    def test_check_known_media_domain_imgur_without_extension():
        """Test imgur URL without file extension."""
        url = "https://imgur.com/example"
        parsed_url = urlparse(url)
        is_media, media_type = check_known_media_domain(parsed_url)

        assert is_media is True
        assert media_type == "image"

    @staticmethod
    def test_check_known_media_domain_gfycat():
        """Test gfycat URL detection."""
        url = "https://gfycat.com/example"
        parsed_url = urlparse(url)
        is_media, media_type = check_known_media_domain(parsed_url)

        assert is_media is True
        assert media_type == "gif"

    @staticmethod
    def test_check_known_media_domain_non_media():
        """Test URL that isn't from a media domain."""
        url = "https://example.com/page"
        parsed_url = urlparse(url)
        is_media, media_type = check_known_media_domain(parsed_url)

        assert is_media is False
        assert media_type is None

    @staticmethod
    def test_is_media_url_empty():
        """Test with empty URL."""
        is_media, media_type = is_media_url("")

        assert is_media is False
        assert media_type is None

    @staticmethod
    def test_is_media_url_none():
        """Test with None URL."""
        is_media, media_type = is_media_url("")

        assert is_media is False
        assert media_type is None

    @staticmethod
    def test_is_media_url_known_domain():
        """Test with URL from known domain."""
        url = "https://i.redd.it/example.jpg"
        is_media, media_type = is_media_url(url)

        assert is_media is True
        assert media_type == "image"

    @staticmethod
    def test_is_media_url_fallback_to_extension():
        """Test fallback to file extension for unknown domain."""
        url = "https://example.com/image.jpg"
        is_media, media_type = is_media_url(url)

        assert is_media is True
        assert media_type == "image"

    @staticmethod
    def test_is_media_url_video_extension():
        """Test detection of video by extension."""
        url = "https://example.com/video.mp4"
        is_media, media_type = is_media_url(url)

        assert is_media is True
        assert media_type == "video"

    @staticmethod
    def test_is_media_url_non_media():
        """Test with non-media URL."""
        url = "https://example.com/page.html"
        is_media, media_type = is_media_url(url)

        assert is_media is False
        assert media_type is None
