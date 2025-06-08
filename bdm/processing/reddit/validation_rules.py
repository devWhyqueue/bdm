import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Optional

from pyspark import Row

from .utils import clean_string, is_valid_uri, parse_iso_datetime_string

logger = logging.getLogger(__name__)

# Constant from the original file, related to validation
MAX_TIME_DIFFERENCE_HOURS = 1  # For created_utc vs scraped_at check


def parse_metadata_scraped_at(metadata: Row) -> Optional[datetime]:
    """
    Extract metadata['scraped_at'] and return it as a datetime.
    Works when `metadata` is a dict, Spark Row, or JSON string.
    Returns None and logs an error if the field is absent or invalid.
    """
    if metadata is None:
        logger.error("metadata is None")
        return None

    # Spark Row → plain dict
    metadata = metadata.asDict(recursive=False)

    # Raw JSON text → dict
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except json.JSONDecodeError:
            logger.error("metadata is not valid JSON text: %s", metadata[:120])
            return None

    if not isinstance(metadata, dict):
        logger.error("metadata has unexpected type %s", type(metadata))
        return None

    scraped_at_str = metadata.get("scraped_at")
    scraped_at_dt = parse_iso_datetime_string(scraped_at_str)

    if scraped_at_dt is None:
        logger.error("Invalid or missing 'scraped_at': %s", scraped_at_str)

    return scraped_at_dt


def validate_id(post_id: Any) -> Optional[str]:
    """Validates post ID: must be alphanumeric string."""
    if not isinstance(post_id, str) or not post_id.isalnum():
        logger.warning(f"Invalid post ID: {post_id}. Returning None.")
        return None
    return post_id


def validate_string_field(value: Any, field_name: str, min_length: int = 1) -> Optional[str]:
    """Validates a string field: must be non-empty after cleaning."""
    cleaned_value = clean_string(value)
    if not cleaned_value or len(cleaned_value) < min_length:
        logger.warning(f"Invalid or empty '{field_name}': {value}. Returning None.")
        return None
    return cleaned_value


def validate_and_convert_created_utc(
        created_utc_unix: Any, scraped_at_dt: datetime
) -> Optional[datetime]:
    """
    • Ensure `created_utc_unix` is a non-negative int/float.
    • Convert it to an offset-aware UTC datetime.
    • Ensure both `created_dt` and `scraped_at_dt` are UTC-aware.
    • Reject values more than MAX_TIME_DIFFERENCE_HOURS after the scrape.
    """
    # 1. basic type / range check
    if not isinstance(created_utc_unix, (int, float)) or created_utc_unix < 0:
        logger.warning("Invalid 'created_utc' value: %s. Returning None.", created_utc_unix)
        return None

    # 2. epoch → aware datetime
    try:
        created_dt = datetime.fromtimestamp(float(created_utc_unix), tz=timezone.utc)
    except (ValueError, TypeError, OverflowError) as e:
        logger.warning("Cannot convert 'created_utc' %s to datetime: %s. Returning None.", created_utc_unix, e)
        return None

    # 3. normalise scraped_at_dt to UTC-aware if it arrived naïve
    if scraped_at_dt.tzinfo is None:
        scraped_at_dt = scraped_at_dt.replace(tzinfo=timezone.utc)

    # 4. plausibility check
    limit = scraped_at_dt + timedelta(hours=MAX_TIME_DIFFERENCE_HOURS)
    if created_dt > limit:
        logger.warning("'created_utc' (%s) is too far after 'scraped_at' (%s). Returning None.", created_dt,
                       scraped_at_dt)
        return None

    return created_dt


def clean_numeric_field(value: Any, field_name: str, default_on_negative: int = 0) -> int:
    """Cleans a numeric field (score, num_comments). Sets to default if negative."""
    if not isinstance(value, int):
        logger.warning(f"'{field_name}' is not an integer: {value}. Using {default_on_negative}.")
        return default_on_negative
    if value < 0:
        logger.warning(f"Negative '{field_name}': {value}. Setting to {default_on_negative}.")
        return default_on_negative
    return value


def validate_url_field(url_str: Any) -> Optional[str]:
    """Validates a URL field."""
    cleaned_url = clean_string(url_str)
    if not cleaned_url or not is_valid_uri(cleaned_url):
        logger.warning(f"Invalid URL: {url_str}. Returning None.")
        return None
    return cleaned_url


def clean_upvote_ratio_field(ratio: Any) -> float:
    """Cleans 'upvote_ratio', clamping to [0,1]."""
    if not isinstance(ratio, (float, int)):
        logger.warning(f"'upvote_ratio' is not a number: {ratio}. Defaulting to 0.0.")
        return 0.0
    if ratio < 0.0:
        logger.warning(f"'upvote_ratio' < 0: {ratio}. Clamping to 0.0.")
        return 0.0
    if ratio > 1.0:
        logger.warning(f"'upvote_ratio' > 1: {ratio}. Clamping to 1.0.")
        return 1.0
    return float(ratio)


def clean_boolean_field(value: Any, field_name: str) -> bool:
    """Cleans a boolean field. Defaults to False if missing/null/invalid type."""
    if isinstance(value, bool):
        return value
    logger.warning(f"Invalid or missing '{field_name}': {value}. Defaulting to False.")
    return False
