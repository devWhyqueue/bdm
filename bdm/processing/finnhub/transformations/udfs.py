import json
import logging
from datetime import datetime
from typing import Any, Dict, Optional

import pyspark.sql.functions as F
from jsonschema import validate as js_validate
from jsonschema.exceptions import ValidationError as JSValidationError
from pyspark.sql.types import (Row, StringType)

from bdm.processing.finnhub.schemas import (
    FINNHUB_FILE_SCHEMA, FINNHUB_ARTICLE_SPARK_SCHEMA, CLEANED_ARTICLE_STRUCT_SCHEMA
)
from bdm.processing.finnhub.transformations.article_processor import validate_and_clean_article
from bdm.processing.finnhub.transformations.utils import parse_datetime_string, generate_checksum

log = logging.getLogger(__name__)


@F.udf(StringType())
def validate_raw_json_udf(raw: str) -> Optional[str]:
    try:
        js_validate(json.loads(raw), FINNHUB_FILE_SCHEMA)
        return raw
    except (json.JSONDecodeError, JSValidationError) as e:
        log.error("Bad raw JSON: %s", e)
    return None


@F.udf(CLEANED_ARTICLE_STRUCT_SCHEMA)
def validate_and_clean_article_udf(
        raw_article: Row,
        metadata_category: str,
        scraped_at_str: str
) -> Optional[Dict[str, Any]]:
    if not raw_article or not metadata_category or not scraped_at_str:
        return None

    scraped_at_dt = parse_datetime_string(scraped_at_str)
    if not scraped_at_dt:
        return None

    return validate_and_clean_article(raw_article.asDict(), metadata_category, scraped_at_dt)


@F.udf(FINNHUB_ARTICLE_SPARK_SCHEMA)
def build_final_article_udf(article: Row,
                            scraped_at_dt: datetime,
                            file_name: str) -> Optional[Dict[str, Any]]:
    if not article or not scraped_at_dt:
        return None

    d = article.asDict(True)

    # 1. Prepare a payload for the checksum with consistent ISO strings.
    checksum_payload = d.copy()
    checksum_payload['published_at_utc'] = d['published_at_utc'].isoformat()
    checksum_payload['scraped_at'] = scraped_at_dt.isoformat()
    checksum_payload['source_file'] = file_name

    checksum = generate_checksum(checksum_payload)

    # 2. Update the original dictionary with the final fields for the Iceberg record.
    d.update(scraped_at=scraped_at_dt, source_file=file_name,
             checksum_sha256=checksum)

    return {f.name: d.get(f.name) for f in FINNHUB_ARTICLE_SPARK_SCHEMA}
