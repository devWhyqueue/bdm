import logging
from datetime import datetime
from typing import Dict, Any

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import col, lit, regexp_extract, input_file_name, row_number
from pyspark.sql.types import StructType, StructField, StringType

from .utils import calculate_sha256_checksum, datetime_to_iso_utc, CHECKSUM_KEY_ORDER

logger = logging.getLogger(__name__)

ICEBERG_TABLE_NAME = "catalog.reddit_posts"  # Default Iceberg table name


def create_spark_session(app_name: str = "RedditProcessingSpark") -> SparkSession:
    """Creates and returns a Spark session configured for Iceberg."""
    spark = SparkSession.builder.appName(app_name) \
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://trusted-zone/iceberg_catalog/") \
        .getOrCreate()
    logger.info(f"Spark session '{app_name}' created.")
    return spark


def read_raw_text_files_with_source(spark: SparkSession, input_s3_path_glob: str) -> DataFrame:
    """Reads raw text files from the given path and adds a source_file_name column."""
    try:
        raw_text_df = spark.read.text(input_s3_path_glob, wholetext=True) \
            .withColumn(
            "source_file_name",
            regexp_extract(input_file_name(), r'[^/]+$', 0)
        )
        logger.info(f"Read {raw_text_df.count()} files from {input_s3_path_glob}")
        return raw_text_df
    except Exception as e:
        logger.error(f"Error reading files from {input_s3_path_glob} with Spark: {e}")
        # Return an empty DataFrame with expected schema to prevent downstream errors
        # It's crucial for the caller to check if the DataFrame is empty.
        schema = StructType([
            StructField("value", StringType(), True),
            StructField("source_file_name", StringType(), True)
        ])
        return spark.createDataFrame([], schema)


def write_df_to_iceberg(spark: SparkSession, posts: DataFrame, table: str, source: str, ) -> bool:
    """
    Upsert *posts* into an Iceberg *table*.

    The newest record for each ``id`` is kept (latest ``scraped_at`` →
    ``created_utc`` → ``title``). The merged result overwrites the target
    table atomically, relying on Iceberg semantics.
    """
    if posts is None or posts.isEmpty():
        logger.info("No rows in %s — skipping upsert.", source)
        return True

    try:
        latest = posts.withColumn(
            "_rn",
            row_number().over(
                Window.partitionBy("id").orderBy(
                    col("scraped_at").desc(),
                    col("created_utc").desc(),
                    col("title"),
                )
            )
        ).filter(col("_rn") == 1).drop("_rn").cache()

        merged = latest.unionByName(
            spark.table(table), allowMissingColumns=True
        ).withColumn(
            "_rn",
            row_number().over(
                Window.partitionBy("id").orderBy(
                    col("scraped_at").desc(),
                    col("created_utc").desc(),
                    col("title"),
                )
            )
        ).filter(col("_rn") == 1).drop("_rn")

        merged.write.mode("overwrite") \
            .option("write.distribution-mode", "hash") \
            .saveAsTable(table)

        logger.info(
            "Merged %d rows from %s into %s.",
            latest.count(), source, table
        )
        return True

    except Exception as exc:
        logger.error(
            "Upsert from %s into %s failed: %s",
            source, table, exc,
            exc_info=True
        )
        return False

    finally:
        if "latest" in locals():
            latest.unpersist(False)


def _reconstruct_checksum_payload(row_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Helper to reconstruct payload for checksum from a Spark row dictionary."""
    payload = {}
    for key in CHECKSUM_KEY_ORDER:
        payload[key] = row_dict.get(key)
    # Ensure timestamps are in ISO UTC string format for checksum
    if isinstance(payload.get("created_utc"), datetime):
        payload["created_utc"] = datetime_to_iso_utc(payload["created_utc"])
    if isinstance(payload.get("scraped_at"), datetime):
        payload["scraped_at"] = datetime_to_iso_utc(payload["scraped_at"])
    return payload


def _verify_partition_record_count(
        partition_df: DataFrame,
        expected_record_count: int,
        source_file_name: str,
        partition_date_str: str,
        table_name: str
) -> bool:
    """Verifies the record count for a given partition DataFrame."""
    actual_record_count = partition_df.count()
    if actual_record_count != expected_record_count:
        logger.error(
            f"Record count mismatch for {source_file_name} in {table_name} "
            f"(partition: {partition_date_str}). Expected: {expected_record_count}, Found: {actual_record_count}."
        )
        return False
    logger.info(f"Record count verified for {source_file_name}: {actual_record_count}.")
    return True


def _verify_sample_checksum(
        partition_df: DataFrame,
        source_file_name: str,
        partition_date_str: str
) -> bool:
    """Verifies the checksum for a sample row from the partition DataFrame."""
    if partition_df.isEmpty():  # Check if DataFrame is empty before sampling
        logger.info(f"No records to sample for checksum verification for {source_file_name} (empty partition_df).")
        return True  # Nothing to verify if no records

    sample_row_spark = partition_df.limit(1).collect()[0]
    sample_row_dict = sample_row_spark.asDict(recursive=True)

    expected_checksum = sample_row_dict.get("checksum_sha256")
    checksum_payload = _reconstruct_checksum_payload(sample_row_dict)
    recalculated_checksum = calculate_sha256_checksum(checksum_payload)

    if expected_checksum != recalculated_checksum:
        logger.error(
            f"Checksum mismatch for sample from {source_file_name} (partition: {partition_date_str}).\n"
            f"Expected: {expected_checksum}, Recalculated: {recalculated_checksum}.\n"
            f"Payload used: {checksum_payload}"
        )
        return False
    logger.info(f"Sample checksum verified for {source_file_name}.")
    return True


def verify_iceberg_write(
        spark: SparkSession,
        table_name: str,
        scraped_at_dt: datetime,  # Partition value
        expected_record_count: int,
        source_file_name: str
) -> bool:
    """Verifies Iceberg write by checking count and a sample checksum."""
    partition_date_str = scraped_at_dt.strftime("%Y-%m-%d")
    logger.info(f"Verifying write for partition: {partition_date_str}, file: {source_file_name}")

    try:
        partition_df = spark.read.table(table_name) \
            .where(col("scraped_at").cast("date") == lit(partition_date_str)) \
            .where(col("source_file") == lit(source_file_name))

        if not _verify_partition_record_count(
                partition_df, expected_record_count, source_file_name, partition_date_str, table_name
        ):
            return False

        # If expected count is 0, count verification is enough.
        if expected_record_count == 0:
            logger.info(f"Expected record count is 0 for {source_file_name}. Skipping checksum.")
            return True

        if not _verify_sample_checksum(partition_df, source_file_name, partition_date_str):
            return False

        return True

    except Exception as e:
        logger.error(f"Error verifying Iceberg write for {source_file_name} (partition: {partition_date_str}): {e}",
                     exc_info=True)
        return False
