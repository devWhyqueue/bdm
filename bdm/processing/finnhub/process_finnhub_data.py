import argparse
import logging
import sys
from datetime import datetime
from typing import Optional, Tuple, List, Dict, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import regexp_extract, input_file_name

from bdm.processing.finnhub.spark_io import (
    ICEBERG_TABLE_NAME,
    write_df_to_iceberg,
    verify_iceberg_write,
)
from bdm.processing.finnhub.transformations import (
    validate_and_parse_raw_df,
    extract_clean_and_transform_articles_df,
    prepare_final_df,
)
from bdm.utils import create_spark_session

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def process_single_file_df(file_content_df: DataFrame, source_file_name: str) \
        -> Tuple[Optional[DataFrame], int, Optional[datetime]]:
    parsed_df = validate_and_parse_raw_df(file_content_df)

    if parsed_df.isEmpty():
        logger.info(f"File {source_file_name}: No data after initial parsing. Skipping.")
        return None, 0, None

    transformed_articles_df = extract_clean_and_transform_articles_df(parsed_df)
    if transformed_articles_df is None or transformed_articles_df.isEmpty():
        logger.warning(f"File {source_file_name}: No articles after transformations and consistency checks. Skipping.")
        return None, 0, None

    n_cleaned_for_file = 0
    if not transformed_articles_df.isEmpty():
        n_cleaned_row = transformed_articles_df.select("N_cleaned").first()
        if n_cleaned_row:
            n_cleaned_for_file = n_cleaned_row["N_cleaned"]

    if n_cleaned_for_file == 0:
        logger.warning(f"File {source_file_name}: N_cleaned is 0. Skipping further processing for this file.")
        return None, 0, None

    final_iceberg_df = prepare_final_df(transformed_articles_df)
    if final_iceberg_df is None or final_iceberg_df.isEmpty():
        logger.warning(f"File {source_file_name}: No articles remaining after final preparation for Iceberg. Skipping.")
        return None, n_cleaned_for_file, None

    scraped_at_dt_for_verification = None
    if not final_iceberg_df.isEmpty():
        scraped_at_dt_for_verification = final_iceberg_df.select("scraped_at").first()["scraped_at"]

    return final_iceberg_df, n_cleaned_for_file, scraped_at_dt_for_verification


def _process_single_file_and_attempt_write(
        spark: SparkSession, source_file: str, table_name: str
) -> Tuple[bool, int, Optional[datetime], str]:
    """
    Processes a single Finnhub data file and attempts to write its cleaned data to Iceberg.

    Returns:
        Tuple containing: (write_success, num_cleaned_records, scraped_at_datetime, source_file_name)
    """
    logger.info(f"File {source_file}: Starting processing.")
    file_content_df = spark.read.text(source_file, wholetext=True)

    final_df_for_file, n_cleaned, scraped_at_dt = process_single_file_df(
        file_content_df, source_file
    )

    if n_cleaned > 0 and final_df_for_file is not None and not final_df_for_file.isEmpty():
        logger.info(f"File {source_file}: {n_cleaned} cleaned records. Attempting to write to Iceberg.")
        write_success = write_df_to_iceberg(final_df_for_file, table_name, source_file)
        if write_success:
            logger.info(f"File {source_file}: Successfully written to Iceberg.")
            return True, n_cleaned, scraped_at_dt, source_file
        logger.error(f"File {source_file}: Failed to write to Iceberg.")
        return False, n_cleaned, scraped_at_dt, source_file  # n_cleaned is relevant for error tracking

    msg = f"File {source_file}: No records to write or processing issue "
    if final_df_for_file is None:
        df_status = "None"
    elif final_df_for_file.isEmpty():
        df_status = "Exists (empty)"
    else:
        df_status = "Exists"

    msg += f"(n_cleaned={n_cleaned}, final_df is {df_status})."
    if n_cleaned > 0 and (final_df_for_file is None or final_df_for_file.isEmpty()):
        logger.error(f"File {source_file}: Discrepancy - {msg}")
    else:
        logger.info(msg)
    return False, n_cleaned, scraped_at_dt, source_file


def _process_files_and_collect_results(
        spark: SparkSession, source_files_list: List[str], table_name: str
) -> Tuple[int, bool, List[Dict[str, Any]]]:
    """
    Iterates through source files, processes each, attempts to write to Iceberg,
    and collects results including total records written and success status.
    """
    total_records_written = 0
    overall_processing_success = True
    successfully_written_files_info: List[Dict[str, Any]] = []

    for source_file in source_files_list:
        write_succeeded, n_cleaned, scraped_at_dt, _ = \
            _process_single_file_and_attempt_write(spark, source_file, table_name)

        if write_succeeded:
            total_records_written += n_cleaned
            if not scraped_at_dt:
                logger.error(f"File {source_file}: Write success but no scraped_at_dt. Critical error.")
                overall_processing_success = False
            else:
                successfully_written_files_info.append({
                    "source_file": source_file, "scraped_at_dt": scraped_at_dt, "n_cleaned": n_cleaned
                })
        elif n_cleaned > 0:
            logger.warning(f"File {source_file}: {n_cleaned} records processed but not written. See previous logs.")
            overall_processing_success = False

    return total_records_written, overall_processing_success, successfully_written_files_info


def _verify_processed_files(
        spark: SparkSession,
        processed_files_info: List[Dict[str, Any]],
        table_name: str
) -> bool:
    """Verifies Iceberg writes for a list of successfully processed files."""
    if not processed_files_info:
        return True

    logger.info("Starting post-insert verification for successfully written files...")
    verification_passed_for_all = True
    for file_info in processed_files_info:
        logger.info(f"Verifying write for file: {file_info['source_file']}")
        verify_success = verify_iceberg_write(
            spark=spark, table_name=table_name,
            scraped_at_dt_object=file_info["scraped_at_dt"],
            source_file_name=file_info["source_file"],
            expected_n_cleaned_for_file=file_info["n_cleaned"]
        )
        if not verify_success:
            logger.error(f"Post-insert verification FAILED for file: {file_info['source_file']}.")
            verification_passed_for_all = False
        else:
            logger.info(f"Post-insert verification SUCCESSFUL for file: {file_info['source_file']}.")

    if verification_passed_for_all:
        logger.info("Post-insert verification passed for all written files.")
    else:
        logger.error("One or more files failed post-insert verification.")
    return verification_passed_for_all


def process_finnhub_data_spark(spark: SparkSession, input_s3_path_glob: str) -> bool:
    """
    Processes Finnhub data from S3: reads, cleans, validates, and writes to Iceberg.
    Orchestrates file listing, per-file processing/writing, and post-insert verification.
    """
    logger.info(f"Starting Finnhub data processing from S3 path: {input_s3_path_glob}")

    try:
        raw_files_df = spark.read.text(input_s3_path_glob, wholetext=True).withColumn(
            "source_file_name", regexp_extract(input_file_name(), r'[^/]+$', 0)
        )
    except Exception as e:
        logger.error(f"Error reading files from S3 path {input_s3_path_glob} using Spark: {e}")
        return False  # Indicate failure if initial read fails

    if raw_files_df.isEmpty():
        logger.info(f"No files found or error reading from {input_s3_path_glob}. Exiting.")
        return True  # No files to process is not an error for the job itself

    source_files_list = [
        row.source_file_name for row in raw_files_df.select("source_file_name").distinct().collect()
    ]

    if not source_files_list:
        logger.info(
            "No source files collected after reading the input path. This might indicate an issue or empty directories.")
        return True

    logger.info(f"Found {len(source_files_list)} files to process: {source_files_list[:5]}... (list truncated if long)")

    total_written, proc_success, written_info = _process_files_and_collect_results(
        spark, source_files_list, ICEBERG_TABLE_NAME
    )
    overall_job_success = proc_success

    if not written_info and total_written == 0:
        logger.warning("No files were successfully processed and written to Iceberg.") if not overall_job_success \
            else logger.info("No new data to write from any of the processed files.")

    if written_info:
        verification_passed = _verify_processed_files(spark, written_info, ICEBERG_TABLE_NAME)
        if not verification_passed:
            overall_job_success = False

    logger.info(
        f"Finnhub data processing finished. Total records written: {total_written}. Overall success: {overall_job_success}")
    return overall_job_success


def parse_cli_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Process Finnhub JSON data from S3 into Iceberg using Spark.")
    parser.add_argument(
        "--input-path", type=str, required=True,
        help="S3 path to JSON files (e.g., s3a://bucket/landing-zone/finnhub/YYYY/MM/DD/*.json)."
    )
    return parser.parse_args()


def main():
    args = parse_cli_args()
    input_s3_path = args.input_path

    logger.info(f"Initializing Spark session for Finnhub processing: {input_s3_path}")

    try:
        spark_session = create_spark_session(app_name="FinnhubProcessingPipeline")
        logger.info("Spark session created successfully.")

        job_success = process_finnhub_data_spark(spark_session, input_s3_path)

        if job_success:
            logger.info("Finnhub data from S3 processed and loaded successfully via Spark.")
            sys.exit(0)
        else:
            logger.error("Issues occurred during Spark processing of Finnhub data. Exiting with failure.")
            sys.exit(1)

    except Exception as e:
        logger.exception(f"An unexpected error occurred in main Finnhub processing: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
