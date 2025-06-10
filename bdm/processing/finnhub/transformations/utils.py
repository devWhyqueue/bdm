"""
Light-weight helpers that carry **no Spark dependencies**.
"""
import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

ISO_Z = "%Y-%m-%dT%H:%M:%S"


def parse_datetime_string(dt: Optional[str]) -> Optional[datetime]:
    if not dt:
        return None
    try:
        obj = datetime.fromisoformat(dt.replace("Z", "+00:00"))
        return obj if obj.tzinfo else obj.replace(tzinfo=timezone.utc)
    except ValueError:
        logger.error("Cannot parse datetime: %s", dt)
        return None


def generate_checksum(record: Dict[str, Any]) -> str:
    """
    Stable SHA-256 over a subset of record fields.
    This function expects all datetime fields to be pre-converted to ISO 8601 strings.
    """
    payload = {
        k: record.get(k)
        for k in (
            "id", "category", "published_at_utc", "headline", "source",
            "summary", "url", "image_url", "scraped_at", "source_file"
        )
    }
    # No custom serializer needed, as we now require strings as input.
    serial = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(serial.encode()).hexdigest()
