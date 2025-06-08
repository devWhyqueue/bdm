import logging
import os

import click

from bdm.ingestion.batch.reddit.post_processor import count_media_stats
from bdm.ingestion.batch.reddit.reddit_api import scrape_subreddit
from bdm.ingestion.batch.reddit.storage import save_to_storage

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def configure_click_options():
    """Set up click command options with environment variable defaults."""

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
    @click.option('--skip-media', is_flag=True, default=lambda: os.environ.get('SKIP_MEDIA', '').lower() == 'true',
                  help='Skip downloading media files')
    def command_wrapper(**kwargs):
        return main(**kwargs)

    return command_wrapper


def main(subreddit: str, limit: int, sort_by: str, time_filter: str,
         bucket: str, prefix: str, skip_media: bool) -> None:
    """Scrape posts from a subreddit and save to MinIO/S3."""
    try:
        logger.info("Starting r/%s scraper (%s)", subreddit, sort_by)
        if skip_media:
            logger.info("Media downloading disabled by --skip-media flag")

        posts = scrape_subreddit(subreddit, sort_by, time_filter, limit, bucket, prefix)
        filename = save_to_storage(posts, bucket, subreddit, sort_by, time_filter, prefix)

        # Log media statistics
        posts_with_media, total_media_items = count_media_stats(posts)
        if not skip_media:
            logger.info("Media download statistics: %d/%d posts with media, %d total media items",
                        posts_with_media, len(posts), total_media_items)

        logger.info("Subreddit scraper completed successfully")
        print(filename)  # Print the filename to stdout
    except Exception as e:
        logger.error("Script failed: %s", str(e))
        raise


if __name__ == "__main__":
    command = configure_click_options()
    command()
