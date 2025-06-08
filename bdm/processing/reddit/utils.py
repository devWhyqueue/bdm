import hashlib
import json
import datetime
from typing import Any, Dict, Optional, Union, List
from urllib.parse import urlparse

# Define the exact order of keys for checksum calculation as per requirements
CHECKSUM_KEY_ORDER: List[str] = [
    "author", "created_utc", "id", "is_original_content", "is_self",
    "num_comments", "permalink", "score", "selftext", "subreddit",
    "scraped_at", "title", "upvote_ratio", "url", "source_file"
]


def clean_string(text: Optional[str]) -> Optional[str]:
    """
    Trims leading/trailing whitespace from a string.
    Returns the cleaned string, or None if the input is None.
    """
    if text is None:
        return None
    return text.strip()


def is_valid_uri(uri_string: Optional[str]) -> bool:
    """
    Validates if a string is a well-formed URI with a scheme and netloc.
    Returns True if valid, False otherwise.
    """
    if not isinstance(uri_string, str) or not uri_string:
        return False
    try:
        result = urlparse(uri_string)
        # Check for essential components of a valid absolute URI
        return all([result.scheme, result.netloc])
    except ValueError:  # Catches errors from urlparse on malformed strings
        return False


def convert_unix_to_iso_utc(unix_timestamp: Optional[Union[int, float, str]]) -> Optional[str]:
    """
    Converts a UNIX timestamp (seconds) to an ISO 8601 UTC timestamp string.
    Example: YYYY-MM-DDTHH:MM:SSZ
    Returns ISO string or None if conversion fails.
    """
    if unix_timestamp is None:
        return None
    try:
        ts = float(unix_timestamp)  # Ensure it's a float for fromtimestamp
        dt_object = datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)
        return dt_object.strftime('%Y-%m-%dT%H:%M:%SZ')
    except (ValueError, TypeError, OverflowError):
        return None


def parse_iso_datetime_string(datetime_str: Optional[str]) -> Optional[datetime.datetime]:
    """
    Parses an ISO 8601 datetime string to a timezone-aware datetime object (UTC).
    Handles 'Z' suffix for UTC. Assumes UTC if no timezone info.
    Returns datetime object or None if parsing fails.
    """
    if not isinstance(datetime_str, str) or not datetime_str:
        return None
    try:
        dt_str_for_parse = datetime_str
        if datetime_str.endswith('Z'):
            # Python's fromisoformat before 3.11 needs +00:00 instead of 'Z'
            dt_str_for_parse = datetime_str[:-1] + '+00:00'

        dt_object = datetime.datetime.fromisoformat(dt_str_for_parse)

        # If parsed object is naive, assume UTC. If aware, convert to UTC.
        if dt_object.tzinfo is None or dt_object.tzinfo.utcoffset(dt_object) is None:
            return dt_object.replace(tzinfo=datetime.timezone.utc)
        return dt_object.astimezone(datetime.timezone.utc)
    except ValueError:
        return None


def datetime_to_iso_utc(dt_object: Optional[datetime.datetime]) -> Optional[str]:
    """
    Converts a datetime object to an ISO 8601 UTC timestamp string.
    Example: YYYY-MM-DDTHH:MM:SSZ
    Returns ISO string or None if input is None. Assumes UTC if naive.
    """
    if dt_object is None:
        return None

    if dt_object.tzinfo is None or dt_object.tzinfo.utcoffset(dt_object) is None:
        dt_object = dt_object.replace(tzinfo=datetime.timezone.utc)
    else:
        dt_object = dt_object.astimezone(datetime.timezone.utc)
    return dt_object.strftime('%Y-%m-%dT%H:%M:%SZ')


def calculate_sha256_checksum(record_data: Dict[str, Any]) -> str:
    """
    Serializes a dictionary to a JSON string with a specific key order
    and computes its SHA-256 hash.
    The input `record_data` must contain all keys specified in `CHECKSUM_KEY_ORDER`.
    Values for 'created_utc' and 'scraped_at' in `record_data` must be ISO8601 UTC strings.
    """
    data_for_json = {}
    for key in CHECKSUM_KEY_ORDER:
        data_for_json[key] = record_data.get(key)

    # Serialize to JSON. `separators` ensures no extra whitespace.
    # `sort_keys=False` is critical as we rely on CHECKSUM_KEY_ORDER.
    # Python 3.7+ dicts preserve insertion order, which is leveraged here.
    json_string = json.dumps(data_for_json, separators=(',', ':'), sort_keys=False)

    sha256_hash = hashlib.sha256(json_string.encode('utf-8')).hexdigest()
    return sha256_hash
