import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List

import pyspark.sql.functions as F
from jsonschema import validate as validate_jsonschema
from jsonschema.exceptions import ValidationError as JsonSchemaValidationError
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, TimestampType, Row, ArrayType

from bdm.processing.finnhub.article_processor import (
    validate_and_clean_article,
    deduplicate_articles,
)
from bdm.processing.finnhub.schemas import (
    FINNHUB_FILE_SCHEMA,
    FINNHUB_ARTICLE_SPARK_SCHEMA,
    SPARK_RAW_FINNHUB_FILE_SCHEMA,
    CLEANED_ARTICLE_STRUCT_SCHEMA,
)

logger = logging.getLogger(__name__)


def parse_datetime_string(datetime_str: Optional[str]) -> Optional[datetime]:
    if not datetime_str:
        return None
    try:
        # Parse ISO 8601 format, assuming UTC if no timezone info
        dt = datetime.fromisoformat(datetime_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        logger.error(f"Could not parse datetime string: {datetime_str}")
        return None


@F.udf(returnType=StringType())
def validate_raw_json_file_udf(json_string: str) -> Optional[str]:
    """Validates a raw JSON string against FINNHUB_FILE_SCHEMA."""
    try:
        data = json.loads(json_string)
        validate_jsonschema(instance=data, schema=FINNHUB_FILE_SCHEMA)
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


@F.udf(returnType=TimestampType())
def parse_metadata_scraped_at_udf(metadata_struct: Row) -> Optional[datetime]:
    if not metadata_struct:
        return None
    scraped_at_str = metadata_struct.scraped_at
    return parse_datetime_string(scraped_at_str)


@F.udf(returnType=ArrayType(CLEANED_ARTICLE_STRUCT_SCHEMA))
def validate_clean_and_deduplicate_articles_udf(
        raw_articles_list: List[Row],
        metadata_category: str,
        scraped_at_dt: datetime
) -> Optional[List[Dict[str, Any]]]:
    if not raw_articles_list or not metadata_category or not scraped_at_dt:
        return None

    cleaned_articles = []
    for raw_article_row in raw_articles_list:
        if raw_article_row is None:
            continue
        raw_article_dict = raw_article_row.asDict(recursive=True)
        cleaned_article = validate_and_clean_article(raw_article_dict, metadata_category, scraped_at_dt)
        if cleaned_article:
            cleaned_articles.append(cleaned_article)

    deduplicated_articles_list = deduplicate_articles(cleaned_articles)

    # Ensure all fields in CLEANED_ARTICLE_STRUCT_SCHEMA are present, adding None if missing
    final_articles_for_struct = []
    for article in deduplicated_articles_list:
        struct_article = {}
        for field in CLEANED_ARTICLE_STRUCT_SCHEMA.fields:
            struct_article[field.name] = article.get(field.name)
        final_articles_for_struct.append(struct_article)

    return final_articles_for_struct


def generate_checksum(article_data: Dict[str, Any], scraped_at_iso: str, source_file: str) -> str:
    checksum_payload = {
        "article_id": article_data.get("article_id"),
        "category": article_data.get("category"),
        "datetime_utc": article_data.get("datetime_utc").isoformat() if article_data.get("datetime_utc") else None,
        "headline": article_data.get("headline"),
        "source": article_data.get("source"),
        "summary": article_data.get("summary", ""),
        "url": article_data.get("url"),
        "image_url": article_data.get("image_url"),
        "scraped_at": scraped_at_iso,
        "source_file": source_file
    }

    serialized_payload = json.dumps(checksum_payload, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(serialized_payload.encode('utf-8')).hexdigest()


@F.udf(returnType=FINNHUB_ARTICLE_SPARK_SCHEMA)
def apply_checksum_and_final_fields_udf(
        cleaned_article_struct: Row,
        scraped_at_datetime: datetime,
        source_file: str
) -> Optional[Dict[str, Any]]:
    if not cleaned_article_struct:
        return None

    cleaned_article_dict = cleaned_article_struct.asDict(recursive=True)

    iceberg_record = {}
    iceberg_record["article_id"] = cleaned_article_dict.get("article_id")
    iceberg_record["category"] = cleaned_article_dict.get("category")
    iceberg_record["datetime_utc"] = cleaned_article_dict.get("datetime_utc")
    iceberg_record["headline"] = cleaned_article_dict.get("headline")
    iceberg_record["source"] = cleaned_article_dict.get("source")
    iceberg_record["summary"] = cleaned_article_dict.get("summary")
    iceberg_record["url"] = cleaned_article_dict.get("url")
    iceberg_record["image_url"] = cleaned_article_dict.get("image_url")

    iceberg_record["scraped_at"] = scraped_at_datetime
    iceberg_record["source_file"] = source_file

    scraped_at_iso_str = scraped_at_datetime.isoformat() if scraped_at_datetime else None
    if not scraped_at_iso_str:
        logger.error("scraped_at_datetime is None during checksum generation.")
        return None

    iceberg_record["checksum_sha256"] = generate_checksum(cleaned_article_dict, scraped_at_iso_str, source_file)

    final_iceberg_record = {}
    for field in FINNHUB_ARTICLE_SPARK_SCHEMA.fields:
        final_iceberg_record[field.name] = iceberg_record.get(field.name)

    return final_iceberg_record


def validate_and_parse_raw_df(raw_text_df_with_source: DataFrame) -> DataFrame:
    validated_df = raw_text_df_with_source.withColumn(
        "valid_json_string", validate_raw_json_file_udf(F.col("value"))
    ).filter(F.col("valid_json_string").isNotNull())

    if validated_df.isEmpty():
        logger.warning("No files passed raw JSON schema validation.")
        return validated_df.sparkSession.createDataFrame([], SPARK_RAW_FINNHUB_FILE_SCHEMA)

    return validated_df.withColumn(
        "parsed_json_content", F.from_json(F.col("valid_json_string"), SPARK_RAW_FINNHUB_FILE_SCHEMA)
    ).select(
        "source_file_name",
        F.col("parsed_json_content.metadata").alias("metadata"),
        F.col("parsed_json_content.news").alias("news_articles_raw_list"),
        F.col("value").alias("original_file_content")
    )


def _parse_metadata_fields(parsed_df: DataFrame) -> Optional[DataFrame]:
    """Parses metadata fields and applies initial filtering."""
    metadata_parsed_df = parsed_df.withColumn(
        "scraped_at_dt", parse_metadata_scraped_at_udf(F.col("metadata"))
    ).withColumn(
        "metadata_category_str", F.col("metadata.category")
    ).withColumn(
        "metadata_article_count", F.col("metadata.article_count").cast("int")
    ).filter(F.col("scraped_at_dt").isNotNull() & F.col("metadata_category_str").isNotNull())

    if metadata_parsed_df.isEmpty():
        logger.warning("No data after parsing scraped_at_dt or metadata_category_str.")
        return None
    return metadata_parsed_df


def _apply_article_cleaning_udf(metadata_parsed_df: DataFrame) -> Optional[DataFrame]:
    """Applies article cleaning UDF and filters empty results."""
    cleaned_articles_df = metadata_parsed_df.withColumn(
        "cleaned_article_struct_list",
        validate_clean_and_deduplicate_articles_udf(
            F.col("news_articles_raw_list"),
            F.col("metadata_category_str"),
            F.col("scraped_at_dt")
        )
    ).filter(F.col("cleaned_article_struct_list").isNotNull() & (F.size(F.col("cleaned_article_struct_list")) > 0))

    if cleaned_articles_df.isEmpty():
        logger.warning("No articles after validation, cleaning, and deduplication.")
        return None
    return cleaned_articles_df


def _perform_count_consistency_check(cleaned_articles_df: DataFrame) -> Optional[DataFrame]:
    """Performs count consistency check and logs skipped files."""
    count_consistency_df = cleaned_articles_df.withColumn(
        "N_cleaned", F.size(F.col("cleaned_article_struct_list"))
    )
    valid_count_df = count_consistency_df.filter(
        F.col("N_cleaned") >= (F.col("metadata_article_count") / 2.0)
    )
    skipped_count_df = count_consistency_df.filter(
        F.col("N_cleaned") < (F.col("metadata_article_count") / 2.0)
    )
    if not skipped_count_df.isEmpty():
        for row in skipped_count_df.select("source_file_name", "N_cleaned", "metadata_article_count").collect():
            logger.warning(
                f"File {row.source_file_name}: N_cleaned ({row.N_cleaned}) "
                f"< metadata_article_count / 2 ({row.metadata_article_count / 2.0}). Skipping."
            )
    if valid_count_df.isEmpty():
        logger.warning("No files passed record-count consistency check.")
        return None
    return valid_count_df


def _explode_articles_and_select_final_fields(valid_count_df: DataFrame) -> Optional[DataFrame]:
    """Explodes articles and selects final DataFrame fields."""
    exploded_df = valid_count_df.select(
        F.col("source_file_name").alias("source_file"),
        F.col("scraped_at_dt"),
        F.col("metadata_category_str"),
        F.col("N_cleaned"),
        F.explode(F.col("cleaned_article_struct_list")).alias("cleaned_article_struct")
    )
    if exploded_df.isEmpty():
        logger.warning("No articles after exploding. This is unexpected.")
        return None
    return exploded_df


def extract_clean_and_transform_articles_df(parsed_df: DataFrame) -> Optional[DataFrame]:
    """Extracts, cleans, and transforms articles from a parsed DataFrame."""
    metadata_df = _parse_metadata_fields(parsed_df)
    if metadata_df is None: return None

    cleaned_df = _apply_article_cleaning_udf(metadata_df)
    if cleaned_df is None: return None

    consistent_df = _perform_count_consistency_check(cleaned_df)
    if consistent_df is None: return None

    final_df = _explode_articles_and_select_final_fields(consistent_df)
    return final_df


def prepare_final_df(transformed_df: DataFrame) -> Optional[DataFrame]:
    if transformed_df.isEmpty():
        logger.warning("Transformed DataFrame is empty. No data to prepare for Iceberg.")
        return None

    final_df_with_struct = transformed_df.withColumn(
        "iceberg_record_struct",
        apply_checksum_and_final_fields_udf(
            F.col("cleaned_article_struct"),
            F.col("scraped_at_dt"),
            F.col("source_file")
        ),
    ).filter(F.col("iceberg_record_struct").isNotNull())

    if final_df_with_struct.isEmpty():
        logger.warning("No records remaining after applying checksum and final fields UDF.")
        return None

    select_fields = [
        F.col(f"iceberg_record_struct.{field.name}").alias(field.name)
        for field in FINNHUB_ARTICLE_SPARK_SCHEMA.fields
    ]
    final_selected_df = final_df_with_struct.select(*select_fields)

    return final_selected_df
