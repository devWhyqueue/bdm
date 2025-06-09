import logging
from typing import Dict, Any, List, Optional, Tuple

from praw.reddit import Submission

from bdm.ingestion.batch.reddit.media_utils import (
    is_media_url, download_media, upload_media_to_s3
)

logger = logging.getLogger(__name__)


def create_media_item(media_type: str, s3_key: str, url: str,
                      filename: str, content_type: str,
                      gallery_item_id: Optional[str] = None) -> Dict[str, Any]:
    """Create a media item dictionary."""
    media_item = {
        'media_type': media_type,
        'lz_url': s3_key,
        'source_url': url,
        'filename': filename,
        'content_type': content_type
    }

    if gallery_item_id:
        media_item['gallery_item_id'] = gallery_item_id

    return media_item


def process_single_media(post: Submission, media_type: str,
                         bucket: str, subreddit: str, prefix: str) -> Optional[Dict[str, Any]]:
    """Process and upload a single media item from a post."""
    media_result = download_media(post.url, media_type)
    if not media_result:
        return None

    filename, content_type, content = media_result
    s3_key = upload_media_to_s3(
        bucket, subreddit, prefix,
        media_type, filename, content, content_type
    )

    if not s3_key:
        return None

    return create_media_item(media_type, s3_key, post.url, filename, content_type)


def process_gallery_image(image_url: str, media_id: str,
                          bucket: str, subreddit: str, prefix: str) -> Optional[Dict[str, Any]]:
    """Process and upload a single gallery image."""
    media_result = download_media(image_url, 'image')
    if not media_result:
        return None

    filename, content_type, content = media_result
    s3_key = upload_media_to_s3(
        bucket, subreddit, prefix,
        'image', filename, content, content_type
    )

    if not s3_key:
        return None

    return create_media_item('image', s3_key, image_url, filename, content_type, media_id)


def process_gallery_post(post: Submission, bucket: str, subreddit: str, prefix: str) -> List[Dict[str, Any]]:
    """Process gallery post and extract all media items."""
    media_items = []

    try:
        if not (hasattr(post, 'is_gallery') and post.is_gallery and
                hasattr(post, 'media_metadata') and post.media_metadata):
            return media_items

        for media_id, media_item in post.media_metadata.items():
            if media_item.get('e') != 'Image' or 's' not in media_item or not media_item['s'].get('u'):
                continue

            image_url = media_item['s']['u']
            gallery_item = process_gallery_image(image_url, media_id, bucket, subreddit, prefix)

            if gallery_item:
                media_items.append(gallery_item)

    except Exception as e:
        logger.warning("Failed to process gallery post %s: %s", post.id, str(e))

    return media_items


def extract_basic_post_data(post: Submission) -> Dict[str, Any]:
    """Extract basic metadata from a Reddit post."""
    return {
        'id': post.id,
        'title': post.title,
        'author': str(post.author),
        'created_utc': post.created_utc,
        'score': post.score,
        'url': post.url,
        'selftext': post.selftext,
        'num_comments': post.num_comments,
        'permalink': post.permalink,
        'upvote_ratio': getattr(post, 'upvote_ratio', None),
        'is_original_content': getattr(post, 'is_original_content', None),
        'is_self': post.is_self
    }


def extract_post_data(post: Submission, bucket: str, subreddit: str, prefix: str) -> Dict[str, Any]:
    """Extract relevant data from a Reddit post and handle media content."""
    post_data = extract_basic_post_data(post)
    media_items = []

    # Process direct media if found
    is_media_result, media_type = is_media_url(post.url)
    if is_media_result and not post.is_self:
        media_item = process_single_media(post, media_type, bucket, subreddit, prefix)
        if media_item:
            media_items.append(media_item)

    # Process gallery posts
    gallery_items = process_gallery_post(post, bucket, subreddit, prefix)
    media_items.extend(gallery_items)

    # Add media items to post data if any were found
    if media_items:
        post_data['media'] = media_items

    return post_data


def count_media_stats(posts: List[Dict[str, Any]]) -> Tuple[int, int]:
    """Count posts with media and total media items."""
    posts_with_media = sum(1 for post in posts if 'media' in post)
    total_media_items = sum(len(post.get('media', [])) for post in posts)
    return posts_with_media, total_media_items
