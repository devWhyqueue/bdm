import logging
import os
from typing import List, Dict, Any, Iterator

import praw
from praw.reddit import Subreddit

from bdm.ingestion.batch.reddit.post_processor import extract_post_data

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


def get_posts_generator(subreddit: Subreddit, sort_by: str, time_filter: str, limit: int) -> Iterator:
    """Get a generator for posts based on sort method."""
    sort_methods = {
        'new': subreddit.new,
        'hot': subreddit.hot,
        'rising': subreddit.rising,
        'top': lambda **kwargs: subreddit.top(time_filter=time_filter, **kwargs),
        'controversial': lambda **kwargs: subreddit.controversial(time_filter=time_filter, **kwargs)
    }

    if sort_by not in sort_methods:
        raise ValueError(f"Invalid sort_by value: {sort_by}")

    return sort_methods[sort_by](limit=limit)


def scrape_subreddit(subreddit_name: str, sort_by: str, time_filter: str,
                     limit: int, bucket: str, prefix: str) -> List[Dict[str, Any]]:
    """Scrape posts from a subreddit based on provided parameters."""
    try:
        reddit = get_reddit_client()
        subreddit = reddit.subreddit(subreddit_name)

        posts_generator = get_posts_generator(subreddit, sort_by, time_filter, limit)
        posts = [extract_post_data(post, bucket, subreddit_name, prefix) for post in posts_generator]

        logger.info("Scraped %d posts from r/%s", len(posts), subreddit_name)
        return posts

    except Exception as e:
        logger.error("Error scraping Reddit: %s", str(e))
        raise
