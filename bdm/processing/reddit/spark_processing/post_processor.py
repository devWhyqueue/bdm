import logging
from datetime import datetime
from typing import Any, Dict, Optional

from pyspark import Row

from bdm.processing.reddit.utils import clean_string
from bdm.processing.reddit.validation_rules import (
    validate_id,
    validate_string_field,
    validate_and_convert_created_utc,
    clean_numeric_field,
    validate_url_field,
    clean_upvote_ratio_field,
    clean_boolean_field,
)

logger = logging.getLogger(__name__)


def _validate_core_post_fields(
        raw_post: Dict[str, Any], scraped_at_dt: datetime
) -> Optional[Dict[str, Any]]:
    """Validates core fields of a post. Returns a dict with validated fields or None."""
    cleaned_post = {}

    post_id = validate_id(raw_post.get("id"))
    if post_id is None: return None
    cleaned_post["id"] = post_id

    title = validate_string_field(raw_post.get("title"), "title")
    if title is None: return None
    cleaned_post["title"] = title

    author = validate_string_field(raw_post.get("author"), "author")
    if author is None: return None
    cleaned_post["author"] = author

    created_dt = validate_and_convert_created_utc(raw_post.get("created_utc"), scraped_at_dt)
    if created_dt is None: return None
    cleaned_post["created_utc"] = created_dt

    url_val = validate_url_field(raw_post.get("url"))
    if url_val is None: return None
    cleaned_post["url"] = url_val

    permalink = validate_string_field(raw_post.get("permalink"), "permalink")
    if permalink is None: return None
    cleaned_post["permalink"] = permalink
    return cleaned_post


def _clean_additional_post_fields(
        raw_post: Dict[str, Any], cleaned_post: Dict[str, Any]
) -> Dict[str, Any]:
    """Cleans additional fields and adds them to the cleaned_post dictionary."""
    cleaned_post["score"] = clean_numeric_field(raw_post.get("score"), "score")
    cleaned_post["num_comments"] = clean_numeric_field(raw_post.get("num_comments"), "num_comments")
    cleaned_post["selftext"] = clean_string(raw_post.get("selftext"))
    cleaned_post["upvote_ratio"] = clean_upvote_ratio_field(raw_post.get("upvote_ratio"))
    cleaned_post["is_original_content"] = clean_boolean_field(
        raw_post.get("is_original_content"), "is_original_content"
    )
    cleaned_post["is_self"] = clean_boolean_field(raw_post.get("is_self"), "is_self")
    return cleaned_post


def validate_and_clean_single_post(
        raw_post: Dict[str, Any], scraped_at_dt: datetime
) -> Optional[Dict[str, Any]]:
    """Validates and cleans a single post record. Returns cleaned post or None."""
    if isinstance(raw_post, Row):  # Spark Row → dict
        raw_post = raw_post.asDict(recursive=False)

    core_fields = _validate_core_post_fields(raw_post, scraped_at_dt)
    if core_fields is None:
        return None

    cleaned_post = _clean_additional_post_fields(raw_post, core_fields)
    return cleaned_post
