import logging
import json
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, regexp_extract, input_file_name, days
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType

# Import the shared function
from bdm.utils import create_spark_session
from bdm.processing.finnhub.transformations import generate_checksum

logger = logging.getLogger(__name__)

ICEBERG_TABLE_NAME = "catalog.finnhub_articles"


# Remove local definition of create_spark_session

def read_raw_json_files_with_source(spark: SparkSession, input_s3_path_glob: str) -> DataFrame:
    '''Reads raw JSON files from the given path as text and adds a source_file_name column.'''
    try:
        raw_text_df = spark.read.text(input_s3_path_glob, wholetext=True)             .withColumn(
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
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    source_description: str
) -> bool:
    if df is None or df.isEmpty():
        logger.info(f"DataFrame from {source_description} is empty. Skipping write to Iceberg table {table_name}.")
        return True

    try:
        df.writeTo(table_name).partitionedBy(days(col("scraped_at"))).append()
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
        "article_id":   row_dict.get("article_id"),
        "category":     row_dict.get("category"),
        "datetime_utc": datetime_utc_iso,
        "headline":     row_dict.get("headline"),
        "source":       row_dict.get("source"),
        "summary":      row_dict.get("summary", ""),
        "url":          row_dict.get("url"),
        "image_url":    row_dict.get("image_url"),
        "scraped_at":   scraped_at_iso,
        "source_file":  row_dict.get("source_file")
    }
    return payload


def verify_iceberg_write(
    spark: SparkSession,
    table_name: str,
    scraped_at_dt_object: datetime,
    source_file_name: str,
    expected_n_cleaned_for_file: int
) -> bool:
    partition_day_str = scraped_at_dt_object.strftime("%Y-%m-%d")
    logger.info(
        f"Verifying Iceberg write for table '{table_name}', "
        f"source_file: '{source_file_name}', partition_day: '{partition_day_str}', "
        f"expected_N_cleaned: {expected_n_cleaned_for_file}"
    )

    try:
        partition_df = spark.read.table(table_name).where(
            (F.date_trunc("day", col("scraped_at")) == F.lit(partition_day_str)) &
            (col("source_file") == lit(source_file_name))
        )

        partition_df.cache()
        actual_record_count = partition_df.count()

        if actual_record_count != expected_n_cleaned_for_file:
            logger.error(
                f"Record count mismatch for file '{source_file_name}' in table '{table_name}' "
                f"(partition day: {partition_day_str}). "
                f"Expected: {expected_n_cleaned_for_file}, Found: {actual_record_count}."
            )
            partition_df.unpersist()
            return False
        logger.info(
            f"Record count verified for file '{source_file_name}' (partition day: {partition_day_str}): "
            f"{actual_record_count} records."
        )

        if expected_n_cleaned_for_file == 0:
            logger.info(f"Expected N_cleaned is 0 for file '{source_file_name}'. Skipping checksum verification.")
            partition_df.unpersist()
            return True

        sample_row_list = partition_df.limit(1).collect()
        if not sample_row_list:
            logger.error(
                f"Cannot verify checksum for file '{source_file_name}' (partition day: {partition_day_str}): "
                f"No records found to sample, but expected {expected_n_cleaned_for_file}."
            )
            partition_df.unpersist()
            return False

        sample_row_spark = sample_row_list[0]
        sample_row_dict = sample_row_spark.asDict(recursive=True)

        expected_checksum = sample_row_dict.get("checksum_sha256")

        scraped_at_iso_for_recalc = sample_row_dict.get("scraped_at").isoformat()

        recalculated_checksum = generate_checksum(
            sample_row_dict,
            scraped_at_iso_for_recalc,
            sample_row_dict.get("source_file")
        )

        if expected_checksum != recalculated_checksum:
            log_payload = _reconstruct_checksum_payload_from_row(sample_row_dict)
            logger.error(
                f"Checksum mismatch for sample from file '{source_file_name}' (partition day: {partition_day_str}).\n"
                f"Article ID: {sample_row_dict.get('article_id')}\n"
                f"Expected Checksum: {expected_checksum}\n"
                f"Recalculated Checksum: {recalculated_checksum}\n"
                f"Payload used for recalculation (derived from row): {json.dumps(log_payload, sort_keys=True, default=str)}"
            )
            partition_df.unpersist()
            return False

        logger.info(
            f"Sample checksum verified for file '{source_file_name}' (partition day: {partition_day_str}). "
            f"Article ID: {sample_row_dict.get('article_id')}"
        )

        partition_df.unpersist()
        return True

    except Exception as e:
        logger.error(
            f"Error verifying Iceberg write for file '{source_file_name}' (partition day: {partition_day_str}): {e}",
            exc_info=True
        )
        if 'partition_df' in locals() and isinstance(partition_df, DataFrame):
            partition_df.unpersist()
        return False
