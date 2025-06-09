import json
import logging
from datetime import datetime
from typing import Any, Dict, Optional

import pyspark.sql.functions as F
from jsonschema import validate as validate_jsonschema
from jsonschema.exceptions import ValidationError as JsonSchemaValidationError
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, TimestampType, Row, ArrayType

from bdm.processing.reddit.media.processor import process_single_media_item
from bdm.processing.reddit.schemas import (
    REDDIT_FILE_SCHEMA,
    REDDIT_POST_SPARK_SCHEMA,
    SPARK_RAW_FILE_SCHEMA,
    CLEANED_POST_STRUCT_SCHEMA,
    MEDIA_ITEM_SCHEMA,
)
from bdm.processing.reddit.spark_processing.post_processor import validate_and_clean_single_post
from bdm.processing.reddit.utils import calculate_sha256_checksum, datetime_to_iso_utc, CHECKSUM_KEY_ORDER
from bdm.processing.reddit.validation_rules import parse_metadata_scraped_at

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
        processed_media_items: Optional[list]
) -> Optional[Dict[str, Any]]:
    """Calculates checksum and prepares the final Iceberg record structure."""
    cleaned_post_struct_dict = cleaned_post_struct.asDict(recursive=False)

    if not cleaned_post_struct_dict:
        return None

    iceberg_record = cleaned_post_struct_dict.copy()
    iceberg_record["subreddit"] = subreddit_name
    iceberg_record["scraped_at"] = scraped_at_datetime
    iceberg_record["source_file"] = source_file
    iceberg_record["media_items"] = processed_media_items

    # Checksum calculation remains the same, based on textual/core post content
    checksum_payload = {}
    for key_to_copy in [
        "id", "title", "author", "score", "selftext", "num_comments",
        "permalink", "upvote_ratio", "is_original_content", "is_self", "url"
    ]:
        checksum_payload[key_to_copy] = iceberg_record.get(key_to_copy)

    # Ensure created_utc and scraped_at are consistently formatted for checksum
    created_utc_val = iceberg_record.get("created_utc")
    if isinstance(created_utc_val, datetime):
        checksum_payload["created_utc"] = datetime_to_iso_utc(created_utc_val)
    else:
        checksum_payload["created_utc"] = created_utc_val

    scraped_at_val = scraped_at_datetime
    if isinstance(scraped_at_val, datetime):
        checksum_payload["scraped_at"] = datetime_to_iso_utc(scraped_at_val)
    else:
        checksum_payload["scraped_at"] = scraped_at_val

    checksum_payload["subreddit"] = subreddit_name
    checksum_payload["source_file"] = source_file

    # Ensure all keys for checksum are present
    for key in CHECKSUM_KEY_ORDER:
        if key not in checksum_payload:
            checksum_payload[key] = None

    iceberg_record["checksum_sha256"] = calculate_sha256_checksum(checksum_payload)

    # Filter out any keys in iceberg_record not in REDDIT_POST_SPARK_SCHEMA
    # This is important because cleaned_post_struct_dict might have more fields initially
    final_iceberg_record = {
        field.name: iceberg_record.get(field.name)
        for field in REDDIT_POST_SPARK_SCHEMA.fields
    }

    return final_iceberg_record


def validate_and_parse_raw_df(raw_text_df_with_source: DataFrame) -> DataFrame:
    """Validates raw JSON strings, parses them, and selects relevant fields."""
    validated_df = raw_text_df_with_source.withColumn(
        "valid_json_string", validate_raw_json_file_udf(F.col("value"))
    ).filter(F.col("valid_json_string").isNotNull())

    if validated_df.isEmpty():
        logger.warning("No files passed raw JSON schema validation.")
        return validated_df

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
        return exploded_df

    return exploded_df.select(
        "source_file_name", "scraped_at_dt", "subreddit_name", F.explode(F.col("posts")).alias("raw_post_data")
    )


@F.udf(returnType=ArrayType(MEDIA_ITEM_SCHEMA))
def process_media_udf(media_array: Optional[list], source_file_name: str) -> Optional[list]:
    """Processes an array of media items for a single post."""
    if media_array is None:
        return None

    processed_media_list = []
    if not media_array:
        return []

    for media_item_struct in media_array:
        if media_item_struct is None:
            logger.warning(f"Encountered null media_item_struct in source_file: {source_file_name}")
            continue

        try:
            media_item_dict = media_item_struct.asDict(recursive=True)

            processed_item_dict = process_single_media_item(media_item_dict, source_file_name)

            if (
                    processed_item_dict.get("validation_error") is None and
                    processed_item_dict.get("conversion_error") is None and
                    processed_item_dict.get("size_error") is None and
                    processed_item_dict.get("tz_url") is not None
            ):
                final_processed_item = {}
                for field in MEDIA_ITEM_SCHEMA.fields:
                    final_processed_item[field.name] = processed_item_dict.get(field.name)
                processed_media_list.append(final_processed_item)
            else:
                logger.warning(
                    f"Media item from {source_file_name} (original s3_url: {media_item_dict.get('lz_url')}) was dropped due to processing errors: "
                    f"Validation: {processed_item_dict.get('validation_error')}, "
                    f"Conversion: {processed_item_dict.get('conversion_error')}, "
                    f"Size: {processed_item_dict.get('size_error')}"
                )

        except Exception as e:
            logger.error(f"Error processing media item in UDF for {source_file_name}: {e}", exc_info=True)
            continue

    return processed_media_list


def clean_and_prepare_final_df(exploded_df: DataFrame) -> Optional[DataFrame]:
    """
    Applies post validation, cleaning, media processing, checksum calculation,
    and prepares the final DataFrame structure for Iceberg.
    """
    if exploded_df.isEmpty():
        logger.warning("Exploded DataFrame is empty. No posts to clean or prepare.")
        return None

    cleaned_posts_df = exploded_df.withColumn(
        "cleaned_post_struct",
        apply_validate_and_clean_post_udf(F.col("raw_post_data"), F.col("scraped_at_dt")),
    ).filter(F.col("cleaned_post_struct").isNotNull())

    if cleaned_posts_df.isEmpty():
        logger.warning("No posts remaining after initial validation and cleaning.")
        return None

    posts_with_processed_media_df = cleaned_posts_df.withColumn(
        "processed_media_items",
        process_media_udf(F.col("raw_post_data.media"), F.col("source_file_name"))
    )

    final_df = posts_with_processed_media_df.withColumn(
        "iceberg_record",
        apply_checksum_and_final_fields_udf(
            F.col("cleaned_post_struct"),
            F.col("subreddit_name"),
            F.col("scraped_at_dt"),
            F.col("source_file_name"),
            F.col("processed_media_items")
        ),
    ).filter(F.col("iceberg_record").isNotNull())

    if final_df.isEmpty():
        logger.warning("No posts remaining after checksum calculation and final field preparation.")
        return None

    select_fields = [F.col(f"iceberg_record.{field.name}").alias(field.name) for field in
                     REDDIT_POST_SPARK_SCHEMA.fields]
    final_selected_df = final_df.select(*select_fields)

    return final_selected_df
