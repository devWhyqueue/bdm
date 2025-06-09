import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional

import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, regexp_extract, input_file_name
from pyspark.sql.types import StructType, StructField, StringType

from bdm.processing.finnhub.transformations import generate_checksum
from bdm.processing.reddit.utils import CHECKSUM_KEY_ORDER

logger = logging.getLogger(__name__)

ICEBERG_TABLE_NAME = "catalog.finnhub_articles"


def read_raw_json_files_with_source(spark: SparkSession, input_s3_path_glob: str) -> DataFrame:
    """Reads raw JSON files from the given path as text and adds a source_file_name column."""
    try:
        raw_text_df = spark.read.text(input_s3_path_glob, wholetext=True).withColumn(
            "source_file_name",
            regexp_extract(input_file_name(), r'[^/]+$', 0)
        )
        count = raw_text_df.count()
        if count == 0:
            logger.info(f"No files found at {input_s3_path_glob}")
        else:
            logger.info(f"Read {count} files from {input_s3_path_glob}")
        return raw_text_df
    except Exception as e:
        logger.error(f"Error reading files from {input_s3_path_glob} with Spark: {e}", exc_info=True)
        schema = StructType([
            StructField("value", StringType(), True),
            StructField("source_file_name", StringType(), True)
        ])
        return spark.createDataFrame([], schema)


def write_df_to_iceberg(
        df: DataFrame,
        table_name: str,
        source_description: str
) -> bool:
    if df is None or df.isEmpty():
        logger.info(f"DataFrame from {source_description} is empty. Skipping write to Iceberg table {table_name}.")
        return True

    try:
        df.writeTo(table_name).append()
        logger.info(
            f"Appended {df.count()} rows from {source_description} into {table_name}."
        )
        return True

    except Exception as exc:
        logger.error(
            f"Write from {source_description} into {table_name} failed: {exc}",
            exc_info=True
        )
        return False


def _reconstruct_checksum_payload_from_row(row_dict: Dict[str, Any]) -> Dict[str, Any]:
    datetime_utc_val = row_dict.get("datetime_utc")
    datetime_utc_iso = datetime_utc_val.isoformat() if isinstance(datetime_utc_val, datetime) else str(datetime_utc_val)

    scraped_at_val = row_dict.get("scraped_at")
    scraped_at_iso = scraped_at_val.isoformat() if isinstance(scraped_at_val, datetime) else str(scraped_at_val)

    payload = {
        "article_id": row_dict.get("article_id"),
        "category": row_dict.get("category"),
        "datetime_utc": datetime_utc_iso,
        "headline": row_dict.get("headline"),
        "source": row_dict.get("source"),
        "summary": row_dict.get("summary", ""),
        "url": row_dict.get("url"),
        "image_url": row_dict.get("image_url"),
        "scraped_at": scraped_at_iso,
        "source_file": row_dict.get("source_file")
    }
    return payload


def _get_partition_data_for_verification(
    spark: SparkSession,
    table_name: str,
        partition_day_str: str,
        source_file_name: str
) -> Optional[DataFrame]:
    """Reads and caches data for a specific partition and source file."""
    try:
        partition_df = spark.read.table(table_name).where(
            (F.date_trunc("day", col("scraped_at")) == F.lit(partition_day_str)) &
            (col("source_file") == lit(source_file_name))
        )
        partition_df.cache()
        return partition_df
    except Exception as e:
        logger.error(
            f"Error reading partition data for {source_file_name} (partition: {partition_day_str}): {e}",
            exc_info=True
        )
        return None


def _check_record_count(
        partition_df: DataFrame,
        expected_count: int,
        source_file_name: str,
        partition_day_str: str
) -> bool:
    """Verifies the record count of the partition DataFrame."""
    actual_count = partition_df.count()
    if actual_count != expected_count:
        logger.error(
            f"Record count mismatch for '{source_file_name}' (partition: {partition_day_str}). "
            f"Expected: {expected_count}, Found: {actual_count}."
        )
        return False
    logger.info(
        f"Record count verified for '{source_file_name}' (partition: {partition_day_str}): {actual_count}."
    )
    return True


def _check_sample_checksum(
        partition_df: DataFrame,
        source_file_name: str,
        partition_day_str: str,
        expected_count: int
) -> bool:
    """Verifies the checksum of a sample row from the partition DataFrame."""
    if expected_count == 0:
        logger.info(f"Expected count is 0 for '{source_file_name}'. Skipping checksum.")
        return True

    sample_row_list = partition_df.limit(1).collect()
    if not sample_row_list:
        logger.error(
            f"Cannot verify checksum for '{source_file_name}' (partition: {partition_day_str}): "
            f"No records to sample, but expected {expected_count}."
        )
        return False

    sample_row_dict = sample_row_list[0].asDict(recursive=True)
    expected_checksum = sample_row_dict.get("checksum_sha256")

    # Use the CHECKSUM_KEY_ORDER for consistent payload construction
    payload_for_recalc = {k: sample_row_dict.get(k) for k in CHECKSUM_KEY_ORDER}
    # Ensure datetime fields are ISO strings for checksum consistency
    if isinstance(payload_for_recalc.get("datetime_utc"), datetime):
        payload_for_recalc["datetime_utc"] = payload_for_recalc["datetime_utc"].isoformat()
    if isinstance(payload_for_recalc.get("scraped_at"), datetime):
        payload_for_recalc["scraped_at"] = payload_for_recalc["scraped_at"].isoformat()

    recalculated_checksum = generate_checksum(
        payload_for_recalc,  # Pass the reconstructed dict
        str(payload_for_recalc["scraped_at"]),  # Ensure it's a string
        str(payload_for_recalc["source_file"])  # Ensure it's a string
    )

    if expected_checksum != recalculated_checksum:
        logger.error(
            f"Checksum mismatch for sample from '{source_file_name}' (partition: {partition_day_str}).\n"
            f"Article ID: {sample_row_dict.get('article_id')}\n"
            f"Expected: {expected_checksum}, Recalculated: {recalculated_checksum}\n"
            f"Payload for recalc: {json.dumps(payload_for_recalc, sort_keys=True, default=str)}"
        )
        return False

    logger.info(
        f"Sample checksum verified for '{source_file_name}' (partition: {partition_day_str}). "
        f"Article ID: {sample_row_dict.get('article_id')}."
    )
    return True


def verify_iceberg_write(
        spark: SparkSession,
        table_name: str,
        scraped_at_dt_object: datetime,
        source_file_name: str,
        expected_n_cleaned_for_file: int
) -> bool:
    """Verifies Iceberg write by checking count and a sample checksum."""
    partition_day_str = scraped_at_dt_object.strftime("%Y-%m-%d")
    log_prefix = f"Verify '{source_file_name}' (partition: {partition_day_str}) for table '{table_name}'"
    logger.info(f"{log_prefix}: Starting. Expected records: {expected_n_cleaned_for_file}.")

    partition_df = _get_partition_data_for_verification(
        spark, table_name, partition_day_str, source_file_name
    )
    if partition_df is None:
        return False  # Error logged in helper

    try:
        if not _check_record_count(
                partition_df, expected_n_cleaned_for_file, source_file_name, partition_day_str
        ):
            return False

        if not _check_sample_checksum(
                partition_df, source_file_name, partition_day_str, expected_n_cleaned_for_file
        ):
            return False

        logger.info(f"{log_prefix}: Verification successful.")
        return True
    except Exception as e:
        logger.error(f"{log_prefix}: Unexpected error during verification steps: {e}", exc_info=True)
        return False
    finally:
        if partition_df is not None:
            partition_df.unpersist()
