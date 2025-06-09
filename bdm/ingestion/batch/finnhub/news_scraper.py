import datetime
import json
import logging
import os
from typing import List, Dict, Any

import click
import requests

from bdm.utils import get_minio_client

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_finnhub_client() -> str:
    """Return the Finnhub API key from environment variables."""
    api_key = os.environ.get('FINNHUB_API_KEY')

    if not api_key:
        raise ValueError("Finnhub API key not found in environment variables")

    return api_key


def fetch_crypto_news(api_key: str, category: str = 'crypto', min_id: int = 0) -> List[Dict[str, Any]]:
    """Fetch crypto news from Finnhub API."""
    url = "https://finnhub.io/api/v1/news"

    params = {
        'category': category,
        'token': api_key,
        'minId': min_id
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        news_data = response.json()

        logger.info("Fetched %d news articles from Finnhub", len(news_data))
        return news_data

    except requests.exceptions.RequestException as e:
        logger.error("Error fetching data from Finnhub: %s", str(e))
        raise


def process_news_data(news_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Process and clean news data from Finnhub."""
    processed_data = []

    for item in news_items:
        processed_item = {
            'id': item.get('id'),
            'category': item.get('category'),
            'datetime': item.get('datetime'),
            'headline': item.get('headline'),
            'source': item.get('source'),
            'summary': item.get('summary'),
            'url': item.get('url'),
            'image': item.get('image'),
            'related': item.get('related')
        }
        processed_data.append(processed_item)

    return processed_data


def generate_filename(category: str, prefix: str) -> str:
    """Generate a timestamped filename for the data."""
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    prefix_path = f"{prefix}/" if prefix else ""
    return f"finnhub/{prefix_path}{category}/{timestamp}.json"


def create_metadata(category: str, news_items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Create metadata object for the saved file."""
    return {
        "category": category,
        "article_count": len(news_items),
        "scraped_at": datetime.datetime.now().isoformat()
    }


def save_to_storage(news_items: List[Dict[str, Any]], bucket: str, category: str, prefix: str) -> str:
    """Save scraped news to MinIO S3 storage."""
    try:
        s3_client = get_minio_client()
        filename = generate_filename(category, prefix)

        metadata = create_metadata(category, news_items)
        data_to_save = {"metadata": metadata, "news": news_items}

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


@click.command()
@click.option('--category', '-c', default=lambda: os.environ.get('NEWS_CATEGORY', 'crypto'),
              help='News category to fetch (default: crypto)')
@click.option('--min-id', type=int,
              default=lambda: int(os.environ.get('MIN_ID', '0')),
              help='Minimum news ID to fetch from')
@click.option('--bucket', '-b', default=lambda: os.environ.get('MINIO_BUCKET', 'finnhub-data'),
              help='S3 bucket name')
@click.option('--prefix', '-p', default=lambda: os.environ.get('OBJECT_PREFIX', ''),
              help='Prefix for S3 object keys')
def main(category: str, min_id: int, bucket: str, prefix: str) -> None:
    """Fetch crypto news from Finnhub and save to MinIO/S3."""
    output_filename = None
    try:
        logger.info("Starting Finnhub %s news scraper", category)

        api_key = get_finnhub_client()
        news_items = fetch_crypto_news(api_key, category, min_id)
        processed_news = process_news_data(news_items)

        output_filename = save_to_storage(processed_news, bucket, category, prefix)
        logger.info("Finnhub news scraper completed successfully")
        if output_filename:
            print(output_filename)

    except Exception as e:
        logger.error("Script failed: %s", str(e))
        raise


if __name__ == "__main__":
    main()
