import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional, List
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# Placeholder for known sources whitelist. This should be managed appropriately.
KNOWN_SOURCES_WHITELIST = [
    "Cointelegraph", "CoinDesk", "Decrypt", "The Block", "CryptoSlate", "Forbes Crypto",
    "Bloomberg Crypto", "Reuters Cryptocurrency", "Wall Street Journal Crypto", "Financial Times Crypto",
    "CNBC Crypto", "Yahoo Finance Crypto", "Business Insider Crypto", "MarketWatch Crypto",
    "Seeking Alpha Crypto", "The Motley Fool Crypto", "Investopedia Crypto", "Bitcoin Magazine",
    "Ethereum World News", "Bitcoin.com News", "CryptoNinjas", "AMBCrypto", "BeInCrypto",
    "Crypto Briefing", "CryptoPotato", "DailyCoin", "DailyHodl", "Finbold", "NewsBTC",
    "NullTX", "The Crypto Basic", "Trustnodes", "UToday", "ZyCrypto", "TechCrunch Crypto",
    "Wired Crypto", "Ars Technica Crypto", "Gizmodo Crypto", "Mashable Crypto", "The Verge Crypto"
]


def is_valid_uri(url: Optional[str]) -> bool:
    if not url:
        return False
    try:
        result = urlparse(url)
        valid_schemes = ['http', 'https']
        return bool(result.scheme in valid_schemes and result.netloc)
    except (ValueError, AttributeError):
        return False


def clean_string(text: Optional[str]) -> Optional[str]:
    if text is None:
        return None
    return text.strip()


# Helper functions
def _validate_article_id(article_id_val: Any) -> Optional[int]:
    """Validates the article ID."""
    # Ensure logger is in scope
    if not isinstance(article_id_val, int) or article_id_val < 0:
        logger.warning(f"Invalid or missing id: {article_id_val}. Dropping record.")
        return None
    return article_id_val


def _validate_article_category(raw_category: Any, metadata_category: str) -> Optional[str]:
    """Validates the article category against metadata."""
    # Ensure clean_string and logger are in scope
    category = clean_string(raw_category)
    if not category or category.lower() != metadata_category.lower():
        logger.warning(
            f"Category mismatch: article category '{category}' vs metadata category '{metadata_category}'. Dropping record.")
        return None
    return category


def _validate_and_convert_article_datetime(
        datetime_unix_val: Any, scraped_at_dt: datetime
) -> Optional[datetime]:
    """Validates and converts the article datetime."""
    # Ensure logger, datetime, timezone, timedelta are in scope
    if not isinstance(datetime_unix_val, int) or datetime_unix_val < 0:
        logger.warning(f"Invalid or missing datetime: {datetime_unix_val}. Dropping record.")
        return None
    try:
        article_dt = datetime.fromtimestamp(datetime_unix_val, timezone.utc)
    except ValueError:
        logger.warning(f"Could not convert UNIX timestamp {datetime_unix_val} to datetime. Dropping record.")
        return None

    if article_dt > (scraped_at_dt + timedelta(hours=1)):
        logger.warning(
            f"Article datetime {article_dt} is more than 1 hour after scraped_at {scraped_at_dt}. Dropping record.")
        return None
    return article_dt


def _validate_article_headline(raw_headline: Any) -> Optional[str]:
    """Validates the article headline."""
    # Ensure clean_string and logger are in scope
    headline = clean_string(raw_headline)
    if not headline or len(headline) < 10:
        logger.warning(f"Invalid or missing headline: '{headline}'. Dropping record.")
        return None
    return headline


def _validate_article_source(raw_source: Any) -> Optional[str]:
    """Validates the article source against a whitelist."""
    # Ensure clean_string, logger, KNOWN_SOURCES_WHITELIST are in scope
    source = clean_string(raw_source)
    if not source or source not in KNOWN_SOURCES_WHITELIST:
        logger.warning(f"Invalid or missing source, or source not in whitelist: '{source}'. Dropping record.")
        return None
    return source


def _validate_article_url(raw_url: Any) -> Optional[str]:
    """Validates the article URL."""
    # Ensure clean_string, is_valid_uri, logger are in scope
    url = clean_string(raw_url)
    if not is_valid_uri(url):
        logger.warning(f"Invalid or missing URL: '{url}'. Dropping record.")
        return None
    return url


def _clean_article_image_url(raw_image_url: Any) -> Optional[str]:
    """Validates and cleans the article image URL."""
    # Ensure clean_string, is_valid_uri, logger are in scope
    image_url = clean_string(raw_image_url)
    if image_url is not None and image_url != "":
        if not is_valid_uri(image_url):
            logger.warning(f"Invalid image URL: '{image_url}'. Setting to null.")
            return None
        return image_url
    return None


def _clean_article_summary(raw_summary: Any) -> str:
    """Cleans the article summary, ensuring it's a string."""
    # Ensure clean_string is in scope
    summary = clean_string(raw_summary)
    return summary if summary is not None else ""


def validate_and_clean_article(
        raw_article: Dict[str, Any],
        metadata_category: str,
        scraped_at_dt: datetime
) -> Optional[Dict[str, Any]]:
    """
    Validates and cleans a raw article dictionary by orchestrating helper functions.

    Each validation step, if failed, results in the article being dropped (returns None).
    Optional fields are cleaned and set appropriately.
    """
    # Assumes Dict, Any, Optional from typing & datetime, timedelta, timezone from datetime are imported.
    # Assumes logger, clean_string, is_valid_uri, KNOWN_SOURCES_WHITELIST are available in scope.
    cleaned_article: Dict[str, Any] = {}

    article_id = _validate_article_id(raw_article.get("id"))
    if article_id is None: return None
    cleaned_article["id"] = article_id

    category = _validate_article_category(raw_article.get("category"), metadata_category)
    if category is None: return None
    cleaned_article["category"] = category

    article_dt = _validate_and_convert_article_datetime(raw_article.get("datetime"), scraped_at_dt)
    if article_dt is None: return None
    cleaned_article["published_at_utc"] = article_dt

    headline = _validate_article_headline(raw_article.get("headline"))
    if headline is None: return None
    cleaned_article["headline"] = headline

    source = _validate_article_source(raw_article.get("source"))
    if source is None: return None
    cleaned_article["source"] = source

    url = _validate_article_url(raw_article.get("url"))
    if url is None: return None
    cleaned_article["url"] = url

    cleaned_article["image_url"] = _clean_article_image_url(raw_article.get("image"))
    cleaned_article["summary"] = _clean_article_summary(raw_article.get("summary"))

    return cleaned_article


def deduplicate_articles(articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not articles:
        return []

    articles_by_id = {}
    for article in articles:
        article_id = article.get("id")  # Using the cleaned field name
        if article_id is None:  # Should not happen if validation passed
            continue

        # datetime_utc is already a datetime object here
        current_article_dt = article.get("published_at_utc")

        if article_id not in articles_by_id or (
                current_article_dt and articles_by_id[article_id].get("published_at_utc") and current_article_dt >
                articles_by_id[article_id]["published_at_utc"]):
            articles_by_id[article_id] = article

    return list(articles_by_id.values())
