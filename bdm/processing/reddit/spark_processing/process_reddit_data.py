import argparse
import logging
import sys
from typing import Optional

from pyspark.sql import SparkSession, DataFrame

from bdm.processing.reddit.spark_processing.spark_io import (
    ICEBERG_TABLE_NAME,
    create_spark_session,
    read_raw_text_files_with_source,
    write_df_to_iceberg,
)
from bdm.processing.reddit.spark_processing.transformations import (
    validate_and_parse_raw_df,
    extract_and_explode_posts_df,
    clean_and_prepare_final_df,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def _ingest_and_parse_raw_data(
        spark: SparkSession, input_s3_path_glob: str
) -> Optional[DataFrame]:
    """Reads raw text files and performs initial validation and parsing."""
    raw_text_df_with_source = read_raw_text_files_with_source(spark, input_s3_path_glob)
    if raw_text_df_with_source.isEmpty():
        logger.info(f"No files found or error reading from {input_s3_path_glob}. Exiting.")
        return None

    parsed_df = validate_and_parse_raw_df(raw_text_df_with_source)
    if parsed_df.isEmpty():
        logger.warning(f"No data after validation and parsing from {input_s3_path_glob}. Exiting.")
        return None
    return parsed_df


def _transform_parsed_data(parsed_df: DataFrame) -> Optional[DataFrame]:
    """Extracts, explodes, cleans, and prepares the final DataFrame."""
    exploded_df = extract_and_explode_posts_df(parsed_df)
    if exploded_df.isEmpty():
        logger.warning("No posts available after exploding or metadata parsing. Exiting.")
        return None

    final_iceberg_df = clean_and_prepare_final_df(exploded_df)
    if final_iceberg_df.isEmpty():
        logger.warning("No posts remaining after final cleaning and preparation. Exiting.")
        return None
    return final_iceberg_df


def _write_final_data_to_iceberg(
        spark: SparkSession, final_df: DataFrame, table_name: str
) -> bool:
    """Caches, counts, and writes the final DataFrame to Iceberg."""
    final_df.cache()
    num_records = final_df.count()
    logger.info(f"Prepared {num_records} records for Iceberg table {table_name}.")

    if num_records == 0:
        logger.info(f"No records to write to Iceberg table {table_name}.")
        final_df.unpersist()
        return True

    success = write_df_to_iceberg(spark, final_df, table_name, "batch_spark_process")
    final_df.unpersist()

    if not success:
        logger.error(f"Failed to write batch to Iceberg table {table_name}.")
        return False

    logger.info(f"Successfully wrote {num_records} records to Iceberg table {table_name}.")
    return True


def process_reddit_data_spark(spark: SparkSession, input_s3_path_glob: str) -> bool:
    """Orchestrates the Reddit data processing pipeline using Spark."""
    parsed_df = _ingest_and_parse_raw_data(spark, input_s3_path_glob)
    if parsed_df is None:
        return True  # No data or error handled in helper, considered successful exit

    final_iceberg_df = _transform_parsed_data(parsed_df)
    if final_iceberg_df is None:
        return True  # No data or error handled in helper, considered successful exit

    success = _write_final_data_to_iceberg(spark, final_iceberg_df, ICEBERG_TABLE_NAME)
    return success


def parse_cli_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Process Reddit JSON data from S3 into Iceberg using Spark.")
    parser.add_argument(
        "--input-path", type=str, required=True,
        help="S3 path to JSON files (e.g., s3a://bucket/path/to/files/ or s3a://bucket/path/to/files/*.json)."
    )
    return parser.parse_args()


def main():
    args = parse_cli_args()
    input_s3_path = args.input_path

    logger.info(f"Initializing Spark session for processing: {input_s3_path}")
    spark_session: Optional[SparkSession] = None

    try:
        spark_session = create_spark_session(app_name="RedditProcessingPipeline")
        logger.info("Spark session created successfully.")

        overall_success = process_reddit_data_spark(spark_session, input_s3_path)

        if overall_success:
            logger.info("All Reddit data from S3 processed and loaded successfully via Spark.")
        else:
            logger.error("Some issues occurred during Spark processing of Reddit data. Exiting with failure.")
            sys.exit(1)

    except Exception as e:
        logger.exception(f"An unexpected error occurred in main: {e}")
        # Ensure any exception turns into a non-zero exit
        sys.exit(1)

    finally:
        if spark_session:
            spark_session.stop()
            logger.info("Spark session stopped.")

    # If we get here, everything passed
    sys.exit(0)


if __name__ == "__main__":
    main()
