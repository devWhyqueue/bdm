import pytest

from bdm.ingestion.batch.reddit.media_utils import download_media, is_media_url

# A known v.redd.it URL that serves an HTML page initially
VREDDIT_URL = "https://v.redd.it/kw5w1lobop5f1"  # Example from user
# A direct image URL for basic testing
DIRECT_IMAGE_URL = "https://i.redd.it/xxwnn82njp5f1.jpeg"  # Example direct image


def test_is_media_url_vreddit():
    """Test that is_media_url correctly identifies v.redd.it URLs as video."""
    is_media, media_type = is_media_url(VREDDIT_URL)
    assert is_media is True
    assert media_type == 'video'


@pytest.mark.online
def test_download_media_direct_image():
    """Test downloading a direct image URL."""
    result = download_media(DIRECT_IMAGE_URL, "image")
    assert result is not None
    filename, content_type, content = result
    assert filename.endswith(".jpeg")
    assert content_type == "image/jpeg"
    assert len(content) > 1000  # Expect a decent size image
    # Check for JPEG magic bytes (SOI marker)
    assert content.startswith(b'\xff\xd8\xff')


@pytest.mark.online  # Mark as online test as it makes external requests
def test_download_vreddit_video_as_actual_video_content():
    """
    Test that download_media can fetch actual video content from a v.redd.it URL,
    not just the HTML page.
    """
    result = download_media(VREDDIT_URL, "video")
    assert result is not None, f"Media download failed for {VREDDIT_URL}. Result was None."
    filename, content_type, content = result

    assert filename.endswith(".mp4"), f"Expected .mp4 extension, got {filename}"
    # Reddit's DASH MP4s sometimes come as 'application/mp4' or 'video/mp4'
    assert "mp4" in content_type or "video/" in content_type, f"Expected video content type (e.g., video/mp4 or application/mp4), got {content_type}"
    assert len(content) > 10000, f"Downloaded content is too small ({len(content)} bytes) to be a video."

    # A simple check to ensure it's not HTML
    content_start_str = content[:200].decode('utf-8', errors='ignore').lower()
    assert "<!doctype html>" not in content_start_str, "Downloaded content appears to be HTML (doctype)."
    assert "<html" not in content_start_str, "Downloaded content appears to be HTML (html tag)."
    assert "<head" not in content_start_str, "Downloaded content appears to be HTML (head tag)."
    assert "<title" not in content_start_str, "Downloaded content appears to be HTML (title tag)."

    # Check for MP4 signature ('ftyp' at offset 4)
    # Common MP4 boxes: ftyp, moov, mdat
    # The 'ftyp' box should be near the beginning.
    assert content[4:8] == b'ftyp', (
        f"MP4 'ftyp' signature not found at expected position. "
        f"Bytes 4-8: {content[4:8]}. First 200 bytes: {content[:200]}"
    )
