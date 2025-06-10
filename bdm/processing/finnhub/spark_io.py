import logging
from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType

from bdm.processing.finnhub.transformations.utils import generate_checksum

logger = logging.getLogger(__name__)
ICEBERG_TABLE = "catalog.finnhub_articles"


def _safe_read(spark: SparkSession, path: str) -> DataFrame:
    """Read text files and add source_file_name."""
    schema = StructType([StructField("value", StringType()), StructField("source_file_name", StringType())])
    try:
        df = (spark.read.text(path, wholetext=True)
              .withColumn("source_file_name", F.regexp_extract(F.input_file_name(), r'[^/]+$', 0)))
        cnt = df.count()
        logger.info(f"{('No' if cnt == 0 else cnt)} files at {path}")
        return df
    except Exception as e:
        logger.error(f"Read error @ {path}: {e}", exc_info=True)
        return spark.createDataFrame([], schema)


def read_raw_json_with_source(spark: SparkSession, path: str) -> DataFrame:
    """Alias for safe read of raw JSON."""
    return _safe_read(spark, path)


def write_to_iceberg(df: DataFrame, table: str, src: str) -> bool:
    """Append DataFrame to Iceberg table if not empty."""
    if not df or df.isEmpty():
        logger.info(f"Empty DF from {src}, skipping {table}")
        return True
    try:
        df.writeTo(table).append()
        logger.info(f"Appended {df.count()} rows from {src} to {table}")
        return True
    except Exception as e:
        logger.error(f"Write failed [{src}->{table}]: {e}", exc_info=True)
        return False


def verify_iceberg_write(spark: SparkSession, table: str, scraped_dt: datetime, src_file: str, expected: int) -> bool:
    """Verify row count and checksum for a given partition."""
    date_str = scraped_dt.strftime("%Y-%m-%d")
    logger.info(f"Verifying {src_file}@{date_str} in {table}, expect {expected}")

    try:
        df = (spark.table(table)
              .where((F.date_trunc("day", F.col("scraped_at")) == date_str)
                     & (F.col("source_file") == src_file)).cache())
    except Exception as e:
        logger.error(f"Load partition error: {e}", exc_info=True)
        return False

    def cnt_check() -> bool:
        """Check record count matches expected."""
        cnt = df.count()
        if cnt != expected:
            logger.error(f"Count mismatch ({expected}/{cnt})")
            return False
        logger.info(f"Count ok ({cnt})")
        return True

    def checksum_check() -> bool:
        """Recalculate and compare sample checksum."""
        if expected == 0:
            logger.info("No records expected, skipping checksum")
            return True
        row = df.limit(1).collect()
        if not row:
            logger.error("No sample row for checksum")
            return False

        rec = row[0].asDict(recursive=True)

        # Prepare a payload with ISO strings, matching the write-path logic.
        checksum_payload = rec.copy()
        checksum_payload['published_at_utc'] = rec['published_at_utc'].isoformat()
        checksum_payload['scraped_at'] = rec['scraped_at'].isoformat()

        recalc = generate_checksum(checksum_payload)

        if rec.get("checksum_sha256") != recalc:
            logger.error(f"Checksum mismatch: {rec.get('checksum_sha256')}/{recalc}")
            return False
        logger.info("Checksum ok")
        return True

    ok = cnt_check() and checksum_check()
    df.unpersist()
    if ok:
        logger.info("Verification successful")
    return ok
