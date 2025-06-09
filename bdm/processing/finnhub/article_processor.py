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
        return all([result.scheme, result.netloc])
    except:
        return False

def clean_string(text: Optional[str]) -> Optional[str]:
    if text is None:
        return None
    return text.strip()

def validate_and_clean_article(
    raw_article: Dict[str, Any],
    metadata_category: str,
    scraped_at_dt: datetime
) -> Optional[Dict[str, Any]]:
    cleaned_article = {}

    # Validate id
    article_id = raw_article.get("id")
    if not isinstance(article_id, int) or article_id < 0:
        logger.warning(f"Invalid or missing id: {article_id}. Dropping record.")
        return None
    cleaned_article["article_id"] = article_id # Map id to article_id

    # Validate category
    category = clean_string(raw_article.get("category"))
    if not category or category.lower() != metadata_category.lower():
        logger.warning(f"Category mismatch: article category '{category}' vs metadata category '{metadata_category}'. Dropping record.")
        return None
    cleaned_article["category"] = category

    # Validate and convert datetime
    datetime_unix = raw_article.get("datetime")
    if not isinstance(datetime_unix, int) or datetime_unix < 0:
        logger.warning(f"Invalid or missing datetime: {datetime_unix}. Dropping record.")
        return None
    try:
        article_dt = datetime.fromtimestamp(datetime_unix, timezone.utc)
    except ValueError:
        logger.warning(f"Could not convert UNIX timestamp {datetime_unix} to datetime. Dropping record.")
        return None

    if article_dt > (scraped_at_dt + timedelta(hours=1)):
        logger.warning(f"Article datetime {article_dt} is more than 1 hour after scraped_at {scraped_at_dt}. Dropping record.")
        return None
    cleaned_article["datetime_utc"] = article_dt

    # Validate headline
    headline = clean_string(raw_article.get("headline"))
    if not headline or len(headline) < 5:
        logger.warning(f"Invalid or missing headline: '{headline}'. Dropping record.")
        return None
    cleaned_article["headline"] = headline

    # Validate source
    source = clean_string(raw_article.get("source"))
    if not source or source not in KNOWN_SOURCES_WHITELIST:
        logger.warning(f"Invalid or missing source, or source not in whitelist: '{source}'. Dropping record.")
        return None
    cleaned_article["source"] = source

    # Validate URL
    url = clean_string(raw_article.get("url"))
    if not is_valid_uri(url):
        logger.warning(f"Invalid or missing URL: '{url}'. Dropping record.")
        return None
    cleaned_article["url"] = url

    # Validate and clean image URL
    image_url = clean_string(raw_article.get("image"))
    if image_url is not None and image_url != "" and not is_valid_uri(image_url):
        logger.warning(f"Invalid image URL: '{image_url}'. Setting to null, but logging issue.")
        cleaned_article["image_url"] = None # Allow null if invalid but log
    elif image_url == "":
        cleaned_article["image_url"] = None
    else:
        cleaned_article["image_url"] = image_url


    # Clean summary
    summary = clean_string(raw_article.get("summary"))
    cleaned_article["summary"] = summary if summary is not None else "" # Ensure empty string if None

    return cleaned_article

def deduplicate_articles(articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not articles:
        return []

    articles_by_id = {}
    for article in articles:
        article_id = article.get("article_id") # Using the cleaned field name
        if article_id is None: # Should not happen if validation passed
            continue

        # datetime_utc is already a datetime object here
        current_article_dt = article.get("datetime_utc")

        if article_id not in articles_by_id or            (current_article_dt and articles_by_id[article_id].get("datetime_utc") and             current_article_dt > articles_by_id[article_id]["datetime_utc"]):
            articles_by_id[article_id] = article

    return list(articles_by_id.values())
