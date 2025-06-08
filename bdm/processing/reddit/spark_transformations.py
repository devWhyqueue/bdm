import json
import logging
from datetime import datetime
from typing import Any, Dict, Optional

import pyspark.sql.functions as F
from jsonschema import validate as validate_jsonschema
from jsonschema.exceptions import ValidationError as JsonSchemaValidationError
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, TimestampType, Row

from .post_processor import validate_and_clean_single_post
from .schemas import (
    REDDIT_FILE_SCHEMA,
    REDDIT_POST_SPARK_SCHEMA,
    SPARK_RAW_FILE_SCHEMA,
    CLEANED_POST_STRUCT_SCHEMA,
)
from .utils import calculate_sha256_checksum, datetime_to_iso_utc, CHECKSUM_KEY_ORDER
from .validation_rules import parse_metadata_scraped_at

logger = logging.getLogger(__name__)


@F.udf(returnType=StringType())
def validate_raw_json_file_udf(json_string: str) -> Optional[str]:
    """Validates a raw JSON string against REDDIT_FILE_SCHEMA."""
    try:
        data = json.loads(json_string)
        validate_jsonschema(instance=data, schema=REDDIT_FILE_SCHEMA)
        return json_string
    except json.JSONDecodeError as e:
        logger.error(f"JSONDecodeError during raw file validation: {e}")
        return None
    except JsonSchemaValidationError as e:
        logger.error(f"JsonSchemaValidationError: {e.message} for file content starting with {json_string[:100]}...")
        return None
    except Exception as e:
        logger.error(f"Unexpected error during raw file validation: {e}")
        return None


parse_metadata_scraped_at_udf = F.udf(parse_metadata_scraped_at, TimestampType())


@F.udf(returnType=CLEANED_POST_STRUCT_SCHEMA)
def apply_validate_and_clean_post_udf(
        raw_post_struct: Dict[str, Any], scraped_at_dt: datetime
) -> Optional[Dict[str, Any]]:
    """Applies post validation and cleaning logic to a raw post struct."""
    if not raw_post_struct or not scraped_at_dt:
        return None
    return validate_and_clean_single_post(raw_post_struct, scraped_at_dt)


@F.udf(returnType=REDDIT_POST_SPARK_SCHEMA)
def apply_checksum_and_final_fields_udf(
        cleaned_post_struct: Row,
        subreddit_name: str,
        scraped_at_datetime: datetime,
        source_file: str,
) -> Optional[Dict[str, Any]]:
    """Calculates checksum and prepares the final Iceberg record structure."""
    cleaned_post_struct = cleaned_post_struct.asDict(recursive=False)

    if not cleaned_post_struct:
        return None
    iceberg_record = cleaned_post_struct.copy()
    iceberg_record["subreddit"] = subreddit_name
    iceberg_record["scraped_at"] = scraped_at_datetime
    iceberg_record["source_file"] = source_file

    checksum_payload = {}
    for key_to_copy in [
        "id", "title", "author", "score", "selftext", "num_comments",
        "permalink", "upvote_ratio", "is_original_content", "is_self", "url"
    ]:
        checksum_payload[key_to_copy] = iceberg_record.get(key_to_copy)

    checksum_payload["created_utc"] = datetime_to_iso_utc(iceberg_record.get("created_utc"))
    checksum_payload["scraped_at"] = datetime_to_iso_utc(scraped_at_datetime)
    checksum_payload["subreddit"] = subreddit_name
    checksum_payload["source_file"] = source_file

    for key in CHECKSUM_KEY_ORDER:
        if key not in checksum_payload:
            checksum_payload[key] = None

    iceberg_record["checksum_sha256"] = calculate_sha256_checksum(checksum_payload)
    return iceberg_record


def validate_and_parse_raw_df(raw_text_df_with_source: DataFrame) -> DataFrame:
    """Validates raw JSON strings, parses them, and selects relevant fields."""
    validated_df = raw_text_df_with_source.withColumn(
        "valid_json_string", validate_raw_json_file_udf(F.col("value"))
    ).filter(F.col("valid_json_string").isNotNull())

    if validated_df.isEmpty():
        logger.warning("No files passed raw JSON schema validation.")
        return validated_df  # Return empty DF with same schema for consistency

    return validated_df.withColumn(
        "parsed_json_content", F.from_json(F.col("valid_json_string"), SPARK_RAW_FILE_SCHEMA)
    ).select(
        "source_file_name",
        F.col("parsed_json_content.metadata").alias("metadata"),
        F.col("parsed_json_content.posts").alias("posts"),
    )


def extract_and_explode_posts_df(parsed_df: DataFrame) -> DataFrame:
    """Extracts metadata, parses scraped_at, and explodes posts array."""
    exploded_df = (
        parsed_df
        .withColumn(
            "scraped_at_dt",
            parse_metadata_scraped_at_udf(F.col("metadata"))
        )
        .filter(F.col("scraped_at_dt").isNotNull())
        .withColumn("subreddit_name", F.col("metadata.subreddit"))
    )

    if exploded_df.isEmpty():
        logger.warning("No data after parsing scraped_at_dt or missing subreddit_name.")
        # Ensure schema consistency for empty df if further specific select occurs
        # For now, Spark handles schema of empty df from filter/withColumn correctly
        return exploded_df

    return exploded_df.select(
        "source_file_name", "scraped_at_dt", "subreddit_name", F.explode(F.col("posts")).alias("raw_post_data")
    )


def clean_and_prepare_final_df(exploded_df: DataFrame) -> DataFrame:
    """
    • Validate & clean each raw post
    • Enrich with checksum + housekeeping fields
    • Flatten the struct so top-level columns match REDDIT_POST_SPARK_SCHEMA
    """
    # 1 ── run post-level validation / cleaning
    cleaned_posts_df = (
        exploded_df
        .withColumn(
            "cleaned_post_struct",
            apply_validate_and_clean_post_udf(
                F.col("raw_post_data"),
                F.col("scraped_at_dt")
            )
        )
        .filter(F.col("cleaned_post_struct").isNotNull())
    )

    if cleaned_posts_df.isEmpty():
        logger.warning("No posts remaining after validation and cleaning.")
        return cleaned_posts_df  # empty DF with same schema

    # 2 ── add checksum + final housekeeping fields (still a struct)
    final_df = cleaned_posts_df.withColumn(
        "iceberg_ready_struct",
        apply_checksum_and_final_fields_udf(
            F.col("cleaned_post_struct"),
            F.col("subreddit_name"),
            F.col("scraped_at_dt"),
            F.col("source_file_name"),
        )
    )

    # 3 ── FLATTEN the struct so Spark can resolve individual columns
    #      and preserve the exact field order defined in the schema
    return final_df.select(
        *[
            F.col(f"iceberg_ready_struct.{field.name}").alias(field.name)
            for field in REDDIT_POST_SPARK_SCHEMA.fields
        ]
    )
