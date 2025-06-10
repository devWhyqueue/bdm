import argparse
import logging
import sys
from datetime import datetime
from typing import List, Dict, Tuple, Any, Optional

import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame

from bdm.processing.finnhub.spark_io import (
    write_to_iceberg, verify_iceberg_write, ICEBERG_TABLE)
from bdm.processing.finnhub.transformations.pipeline import (
    validate_and_parse_raw_df, extract_clean_articles, prepare_final_df)
from bdm.utils import create_spark_session

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)


# ─────────────────── helpers ────────────────────
def _transform_file(spark: SparkSession, path: str) -> Tuple[Optional[DataFrame], int]:
    """
    Full Spark transformation chain.
    Returns the Iceberg-ready DF and the expected row count.
    """
    parsed = validate_and_parse_raw_df(spark.read.text(path, wholetext=True))
    if parsed.isEmpty():
        return None, 0

    cleaned = extract_clean_articles(parsed)
    if cleaned is None or cleaned.isEmpty():
        return None, 0

    # Cache the 'cleaned' DataFrame as it's used for counting and final prep.
    cleaned.cache()

    # CORRECTED LOGIC: The expected count is a direct count of the cleaned DataFrame.
    n_exp = cleaned.count()
    if n_exp == 0:
        cleaned.unpersist()
        return None, 0

    final = prepare_final_df(cleaned)

    # Unpersist the cached DataFrame once we're done with it.
    cleaned.unpersist()

    return final, n_exp if final else (None, 0)


def _write_file(df: DataFrame, path: str, table: str) -> Tuple[bool, int, datetime]:
    """Write DF to Iceberg and report status."""
    # The checksum and source_file columns are now correctly calculated in the final DF.
    # The line that incorrectly overwrote the `source_file` column has been removed.

    n_rows = df.count()
    if n_rows == 0:
        log.info(f"Skipping write for {path} as it produced 0 valid rows.")
        return True, 0, None

    ok = write_to_iceberg(df, table, path)
    scraped_at = df.select("scraped_at").first()["scraped_at"] if ok else None
    return ok, n_rows, scraped_at


# ─────────────────── per-batch logic ────────────────────
def _process_files(spark: SparkSession, files: List[str]) -> Tuple[int, List[Dict[str, Any]]]:
    total, ok_info = 0, []
    for f in files:
        log.info("⇢ %s", f)
        ice_df, n_exp = _transform_file(spark, f)
        if ice_df is None or ice_df.isEmpty():
            log.warning("File %s produced no output.", f)
            continue

        # Handle potential duplicates that might arise from source data issues
        ice_df_deduped = ice_df.dropDuplicates()

        ok, n_written, ts = _write_file(ice_df_deduped, f, ICEBERG_TABLE)
        if ok:
            total += n_written
            # Ensure we have a timestamp before adding to verification list
            if ts:
                ok_info.append({"source_file": f, "scraped_at_dt": ts, "n_cleaned": n_written})

            if n_written != n_exp:
                log.warning("Duplicate rows dropped for %s: %d → %d.", f, n_exp, n_written)
        else:
            log.error("Iceberg write failed for %s.", f)
    return total, ok_info


def _verify(spark: SparkSession, info: List[Dict[str, Any]]) -> bool:
    """True iff every per-file verification passes."""
    return all(
        verify_iceberg_write(
            spark, ICEBERG_TABLE, i["scraped_at_dt"], i["source_file"], i["n_cleaned"])
        for i in info
    ) if info else True


# ─────────────────── driver ────────────────────
def run_job(spark: SparkSession, glob: str) -> bool:
    files = [
        r.source_file_path
        for r in (spark.read.text(glob, wholetext=True)
                  .withColumn("source_file_path", F.input_file_name())
                  .select("source_file_path").distinct().collect())
    ]
    if not files:
        log.info("No input matched %s", glob)
        return True

    log.info("Discovered %d objects (first 5 shown): %s …", len(files), files[:5])
    written, ok_info = _process_files(spark, files)
    verified = _verify(spark, ok_info)
    log.info("Batch summary — rows written: %d, verification OK: %s", written, verified)
    return verified


# ─────────────────── CLI ────────────────────
def _parse_cli() -> str:
    p = argparse.ArgumentParser(description="Finnhub → Iceberg ETL.")
    p.add_argument("--input-path", required=True,
                   help="S3 URI(s) e.g. s3a://bucket/finnhub/2025/06/10/*.json")
    return p.parse_args().input_path


def main() -> None:
    try:
        spark = create_spark_session("FinnhubProcessing")
        ok = run_job(spark, _parse_cli())
        sys.exit(0 if ok else 1)
    except Exception as err:
        log.exception("Fatal: %s", err)
        sys.exit(1)


if __name__ == "__main__":
    main()
