import logging
import os
import uuid
from typing import Optional, Tuple
from urllib.parse import urlparse

import requests

from bdm.ingestion.batch.utils import get_minio_client

logger = logging.getLogger(__name__)

# Media file extensions and MIME types
IMAGE_EXTENSIONS = ['.jpg', '.jpeg', '.png', '.gif', '.webp']
VIDEO_EXTENSIONS = ['.mp4', '.webm', '.mov', '.avi']
# Media content hosts commonly used on Reddit
KNOWN_MEDIA_DOMAINS = [
    'i.redd.it', 'i.imgur.com', 'imgur.com', 'v.redd.it', 'gfycat.com',
    'redgifs.com', 'giphy.com', 'media.giphy.com', 'i.giphy.com'
]


def check_known_media_domain(parsed_url) -> Tuple[bool, Optional[str]]:
    """Check if URL is from a known media domain and determine its type."""
    path = parsed_url.path.lower()

    # Direct image links
    if any(path.endswith(ext) for ext in IMAGE_EXTENSIONS):
        return True, 'image' if '.gif' not in path else 'gif'

    # Direct video links
    if any(path.endswith(ext) for ext in VIDEO_EXTENSIONS):
        return True, 'video'

    # Handle imgur URLs without extensions
    if 'imgur.com' in parsed_url.netloc and not parsed_url.path.endswith(('.jpg', '.png', '.gif')):
        return True, 'image'

    # v.redd.it is always video
    if 'v.redd.it' in parsed_url.netloc:
        return True, 'video'

    # Handle gfycat and similar
    if any(domain in parsed_url.netloc for domain in ['gfycat.com', 'redgifs.com', 'giphy.com']):
        return True, 'gif'

    return False, None


def is_media_url(url: str) -> Tuple[bool, Optional[str]]:
    """Determine if a URL points to media content and identify its type."""
    if not url:
        return False, None

    parsed_url = urlparse(url)

    # Check if it's a known media domain
    if any(domain in parsed_url.netloc for domain in KNOWN_MEDIA_DOMAINS):
        return check_known_media_domain(parsed_url)

    # Check file extension in URL as fallback
    if any(parsed_url.path.lower().endswith(ext) for ext in IMAGE_EXTENSIONS):
        return True, 'image' if '.gif' not in parsed_url.path.lower() else 'gif'

    if any(parsed_url.path.lower().endswith(ext) for ext in VIDEO_EXTENSIONS):
        return True, 'video'

    return False, None


def get_file_extension(url: str, media_type: str) -> str:
    """Determine appropriate file extension based on URL and media type."""
    parsed_url = urlparse(url)
    file_extension = os.path.splitext(parsed_url.path)[1].lower()

    if file_extension and file_extension in IMAGE_EXTENSIONS + VIDEO_EXTENSIONS:
        return file_extension

    # Default extensions by media type
    if media_type == 'image':
        return '.jpg'
    elif media_type == 'gif':
        return '.gif'
    elif media_type == 'video':
        return '.mp4'

    return '.jpg'  # Default fallback


def get_content_type(file_extension: str) -> str:
    """Determine content MIME type from file extension."""
    if file_extension == '.png':
        return 'image/png'
    elif file_extension == '.gif':
        return 'image/gif'
    elif file_extension == '.webp':
        return 'image/webp'
    elif file_extension in VIDEO_EXTENSIONS:
        return 'video/mp4' if file_extension == '.mp4' else 'video/webm'

    return 'image/jpeg'  # Default


def download_media(url: str, media_type: str) -> Optional[Tuple[str, str, bytes]]:
    """Download media from a URL."""
    try:
        file_extension = get_file_extension(url, media_type)
        filename = f"{uuid.uuid4().hex}{file_extension}"
        content_type = get_content_type(file_extension)

        logger.info("Downloading media from %s", url)
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        # Check if we received valid content
        if len(response.content) < 100:
            logger.warning("Media download from %s produced suspiciously small file: %d bytes",
                           url, len(response.content))
            return None

        return filename, content_type, response.content

    except Exception as e:
        logger.warning("Failed to download media from %s: %s", url, str(e))
        return None


def get_s3_key(subreddit: str, prefix: str, media_type: str, filename: str) -> str:
    """Generate S3 object key for media file."""
    prefix_path = f"{prefix}/" if prefix else ""
    media_folder = media_type + 's'  # Convert to plural (image -> images)
    return f"reddit/{prefix_path}{subreddit}/{media_folder}/{filename}"


def upload_media_to_s3(bucket: str, subreddit: str, prefix: str,
                       media_type: str, filename: str, content: bytes,
                       content_type: str) -> Optional[str]:
    """Upload media file to S3 storage."""
    try:
        s3_client = get_minio_client()
        key = get_s3_key(subreddit, prefix, media_type, filename)

        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=content,
            ContentType=content_type
        )

        logger.info("Successfully uploaded %s to %s/%s", media_type, bucket, key)
        return key

    except Exception as e:
        logger.error("Error uploading media to S3: %s", str(e))
        return None
