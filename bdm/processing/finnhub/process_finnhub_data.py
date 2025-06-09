import argparse
import logging
import sys
from typing import Optional, Tuple, List, Dict, Any

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F # Import F for lit
from pyspark.sql.functions import lit

from bdm.processing.finnhub.spark_io import (
    ICEBERG_TABLE_NAME,
    create_spark_session,
    read_raw_json_files_with_source,
    write_df_to_iceberg,
    verify_iceberg_write,
)
from bdm.processing.finnhub.transformations import (
    validate_and_parse_raw_df,
    extract_clean_and_transform_articles_df,
    prepare_final_df,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def process_single_file_df(
    spark: SparkSession, file_content_df: DataFrame, source_file_name: str
) -> Tuple[Optional[DataFrame], int]:
    parsed_df = file_content_df

    if parsed_df.isEmpty():
        logger.info(f"File {source_file_name}: No data after initial parsing. Skipping.")
        return None, 0

    transformed_articles_df = extract_clean_and_transform_articles_df(parsed_df)
    if transformed_articles_df is None or transformed_articles_df.isEmpty():
        logger.warning(f"File {source_file_name}: No articles after transformations and consistency checks. Skipping.")
        return None, 0

    n_cleaned_for_file = 0
    if not transformed_articles_df.isEmpty():
        n_cleaned_row = transformed_articles_df.select("N_cleaned").first()
        if n_cleaned_row:
            n_cleaned_for_file = n_cleaned_row["N_cleaned"]

    if n_cleaned_for_file == 0 :
        logger.warning(f"File {source_file_name}: N_cleaned is 0. Skipping further processing for this file.")
        return None, 0

    final_iceberg_df = prepare_final_df(transformed_articles_df)
    if final_iceberg_df is None or final_iceberg_df.isEmpty():
        logger.warning(f"File {source_file_name}: No articles remaining after final preparation for Iceberg. Skipping.")
        return None, n_cleaned_for_file

    return final_iceberg_df, n_cleaned_for_file


def process_finnhub_data_spark(spark: SparkSession, input_s3_path_glob: str) -> bool:
    raw_text_df_with_source = read_raw_json_files_with_source(spark, input_s3_path_glob)
    if raw_text_df_with_source.isEmpty():
        logger.info(f"No files found or error reading from {input_s3_path_glob}. Exiting.")
        return True

    source_files = [row.source_file_name for row in raw_text_df_with_source.select("source_file_name").distinct().collect()]
    if not source_files:
        logger.info("No source files collected from the input path. Exiting.")
        return True

    logger.info(f"Found {len(source_files)} files to process: {source_files}")

    overall_success = True
    total_records_written = 0

    processed_files_info: List[Dict[str, Any]] = []

    for source_file in source_files:
        logger.info(f"Processing file: {source_file}")

        single_file_raw_text_df = raw_text_df_with_source.filter(F.col("source_file_name") == lit(source_file))

        parsed_df_for_file = validate_and_parse_raw_df(single_file_raw_text_df)
        if parsed_df_for_file.isEmpty():
            logger.warning(f"File {source_file}: Failed initial validation or parsing. Skipping.")
            continue

        final_df_for_file, n_cleaned = process_single_file_df(spark, parsed_df_for_file, source_file)

        if final_df_for_file and not final_df_for_file.isEmpty() and n_cleaned > 0:
            scraped_at_dt_for_verification = None
            if not final_df_for_file.isEmpty():
                 scraped_at_dt_for_verification = final_df_for_file.select("scraped_at").first()["scraped_at"]

            logger.info(f"File {source_file}: Prepared {n_cleaned} records for Iceberg.")
            if scraped_at_dt_for_verification:
                 logger.info(f"File {source_file}: Using scraped_at {scraped_at_dt_for_verification.isoformat()} for partitioning and verification.")

            write_success = write_df_to_iceberg(spark, final_df_for_file, ICEBERG_TABLE_NAME, f"Finnhub-{source_file}")

            if write_success:
                total_records_written += n_cleaned
                if scraped_at_dt_for_verification:
                    processed_files_info.append({
                        "source_file": source_file,
                        "scraped_at_dt": scraped_at_dt_for_verification,
                        "n_cleaned": n_cleaned
                    })
                else:
                    logger.error(f"File {source_file}: Could not retrieve scraped_at_dt for verification, though write was successful.")
                    overall_success = False
            else:
                logger.error(f"File {source_file}: Failed to write to Iceberg.")
                overall_success = False
        elif n_cleaned == 0 and final_df_for_file is None:
            logger.info(f"File {source_file}: Skipped due to data quality/consistency checks (N_cleaned=0).")
        else:
            logger.warning(f"File {source_file}: No records to write after processing. N_cleaned={n_cleaned}.")
            if n_cleaned > 0 and (final_df_for_file is None or final_df_for_file.isEmpty()):
                logger.error(f"File {source_file}: Discrepancy - N_cleaned={n_cleaned} but no final DataFrame. Marking as error.")
                overall_success = False

    if not processed_files_info and total_records_written == 0 and not overall_success:
         logger.warning("No files were successfully processed and written to Iceberg.")
    elif not processed_files_info and total_records_written == 0 and overall_success:
        logger.info("No new data to write from any of the processed files.")

    if processed_files_info:
        logger.info("Starting post-insert verification for successfully written files...")
        verification_passed_for_all_written = True
        for file_info in processed_files_info:
            logger.info(f"Verifying write for file: {file_info['source_file']}")
            verify_success = verify_iceberg_write(
                spark=spark,
                table_name=ICEBERG_TABLE_NAME,
                scraped_at_dt_object=file_info["scraped_at_dt"],
                source_file_name=file_info["source_file"],
                expected_n_cleaned_for_file=file_info["n_cleaned"]
            )
            if not verify_success:
                logger.error(f"Post-insert verification failed for file: {file_info['source_file']}.")
                verification_passed_for_all_written = False
                overall_success = False
            else:
                logger.info(f"Post-insert verification successful for file: {file_info['source_file']}.")

        if verification_passed_for_all_written:
            logger.info("Post-insert verification passed for all written files.")
        else:
            logger.error("One or more files failed post-insert verification.")

    logger.info(f"Finnhub data processing complete. Total records written: {total_records_written}. Overall success: {overall_success}")
    return overall_success


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
    spark_session: Optional[SparkSession] = None

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

    finally:
        if spark_session:
            spark_session.stop()
            logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()
