import datetime
import json
import logging
from typing import Dict, Any, List

from bdm.ingestion.batch.reddit.post_processor import count_media_stats
from bdm.ingestion.batch.utils import get_minio_client

logger = logging.getLogger(__name__)


def generate_filename(subreddit: str, prefix: str) -> str:
    """Generate a timestamped filename for the data."""
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    prefix_path = f"{prefix}/" if prefix else ""
    return f"reddit/{prefix_path}{subreddit}/{timestamp}.json"


def create_metadata(subreddit: str, sort_by: str, time_filter: str, posts: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Create metadata object for the saved file."""
    posts_with_media, total_media_items = count_media_stats(posts)

    return {
        "subreddit": subreddit,
        "sort_by": sort_by,
        "time_filter": time_filter if sort_by in ['top', 'controversial'] else None,
        "post_count": len(posts),
        "posts_with_media": posts_with_media,
        "total_media_items": total_media_items,
        "scraped_at": datetime.datetime.now().isoformat()
    }


def save_to_storage(posts: List[Dict[str, Any]], bucket: str, subreddit: str,
                    sort_by: str, time_filter: str, prefix: str) -> str:
    """Save scraped posts to MinIO S3 storage."""
    try:
        s3_client = get_minio_client()
        filename = generate_filename(subreddit, prefix)
        metadata = create_metadata(subreddit, sort_by, time_filter, posts)

        data_to_save = {"metadata": metadata, "posts": posts}
        s3_client.put_object(
            Bucket=bucket,
            Key=filename,
            Body=json.dumps(data_to_save, indent=2),
            ContentType='application/json'
        )

        logger.info("Successfully saved data to %s/%s", bucket, filename)
        return filename

    except Exception as e:
        logger.error("Error saving to storage: %s", str(e))
        raise
