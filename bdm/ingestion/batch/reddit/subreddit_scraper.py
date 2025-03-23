import datetime
import json
import logging
import os
from typing import List, Dict, Any, Iterator

import click
import praw
from praw.reddit import Submission, Subreddit

from bdm.ingestion.batch.utils import get_minio_client

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_reddit_client() -> praw.Reddit:
    """Initialize and return a Reddit API client."""
    client_id = os.environ.get('REDDIT_CLIENT_ID')
    client_secret = os.environ.get('REDDIT_CLIENT_SECRET')
    user_agent = os.environ.get('REDDIT_USER_AGENT')

    if not client_id or not client_secret:
        raise ValueError("Reddit API credentials not found in environment variables")

    return praw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        user_agent=user_agent
    )


def extract_post_data(post: Submission) -> Dict[str, Any]:
    """Extract relevant data from a Reddit post."""
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


def get_posts_generator(subreddit: Subreddit, sort_by: str, time_filter: str, limit: int) -> Iterator:
    """Get a generator for posts based on sort method."""
    if sort_by == 'new':
        return subreddit.new(limit=limit)
    elif sort_by == 'hot':
        return subreddit.hot(limit=limit)
    elif sort_by == 'rising':
        return subreddit.rising(limit=limit)
    elif sort_by == 'top':
        return subreddit.top(time_filter=time_filter, limit=limit)
    elif sort_by == 'controversial':
        return subreddit.controversial(time_filter=time_filter, limit=limit)
    else:
        raise ValueError(f"Invalid sort_by value: {sort_by}")


def scrape_subreddit(subreddit_name: str, sort_by: str, time_filter: str, limit: int) -> List[Dict[str, Any]]:
    """Scrape posts from a subreddit based on provided parameters."""
    try:
        reddit = get_reddit_client()
        subreddit = reddit.subreddit(subreddit_name)

        posts_generator = get_posts_generator(subreddit, sort_by, time_filter, limit)
        posts = [extract_post_data(post) for post in posts_generator]

        logger.info("Scraped %d posts from r/%s", len(posts), subreddit_name)
        return posts

    except Exception as e:
        logger.error("Error scraping Reddit: %s", str(e))
        raise


def generate_filename(subreddit: str, prefix: str) -> str:
    """Generate a timestamped filename for the data."""
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    prefix_path = f"{prefix}/" if prefix else ""
    return f"reddit/{prefix_path}{subreddit}/{timestamp}.json"


def create_metadata(subreddit: str, sort_by: str, time_filter: str, posts: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Create metadata object for the saved file."""
    return {
        "subreddit": subreddit,
        "sort_by": sort_by,
        "time_filter": time_filter if sort_by in ['top', 'controversial'] else None,
        "post_count": len(posts),
        "scraped_at": datetime.datetime.now().isoformat()
    }


def save_to_storage(posts: List[Dict[str, Any]], bucket: str, subreddit: str, sort_by: str, time_filter: str,
                    prefix: str) -> None:
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

    except Exception as e:
        logger.error("Error saving to storage: %s", str(e))
        raise


@click.command()
@click.option('--subreddit', '-s', default=lambda: os.environ.get('SUBREDDIT', 'bitcoin'),
              help='Subreddit name to scrape')
@click.option('--limit', '-l', type=int,
              default=lambda: int(os.environ.get('POST_LIMIT', '25')),
              help='Maximum number of posts to scrape')
@click.option('--sort-by', type=click.Choice(['new', 'hot', 'top', 'rising', 'controversial']),
              default=lambda: os.environ.get('SORT_BY', 'new'),
              help='Method to sort posts')
@click.option('--time-filter', type=click.Choice(['hour', 'day', 'week', 'month', 'year', 'all']),
              default=lambda: os.environ.get('TIME_FILTER', 'all'),
              help='Time filter for top/controversial posts')
@click.option('--bucket', '-b', default=lambda: os.environ.get('MINIO_BUCKET', 'reddit-data'),
              help='S3 bucket name')
@click.option('--prefix', '-p', default=lambda: os.environ.get('OBJECT_PREFIX', ''),
              help='Prefix for S3 object keys')
def main(subreddit: str, limit: int, sort_by: str, time_filter: str, bucket: str, prefix: str) -> None:
    """Scrape posts from a subreddit and save to MinIO/S3."""
    try:
        logger.info("Starting r/%s scraper (%s)", subreddit, sort_by)
        posts = scrape_subreddit(subreddit, sort_by, time_filter, limit)
        save_to_storage(posts, bucket, subreddit, sort_by, time_filter, prefix)
        logger.info("Subreddit scraper completed successfully")
    except Exception as e:
        logger.error("Script failed: %s", str(e))
        raise


if __name__ == "__main__":
    main()
