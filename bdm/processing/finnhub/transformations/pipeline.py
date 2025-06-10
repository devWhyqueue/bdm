import logging
from typing import Optional

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from bdm.processing.finnhub.schemas import SPARK_RAW_FINNHUB_FILE_SCHEMA, FINNHUB_ARTICLE_SPARK_SCHEMA
from bdm.processing.finnhub.transformations.udfs import (
    validate_raw_json_udf, build_final_article_udf, validate_and_clean_article_udf
)

log = logging.getLogger(__name__)


# ─────────────────────────── public API ────────────────────────────────
def validate_and_parse_raw_df(df: DataFrame) -> DataFrame:
    src = (df
           .withColumn("source_file_path", F.input_file_name())
           .withColumn("raw", validate_raw_json_udf("value"))
           .filter("raw is not null")
           .withColumn("js", F.from_json("raw", SPARK_RAW_FINNHUB_FILE_SCHEMA))
           .selectExpr("source_file_path",
                       "js.metadata as meta",
                       "js.news as news"))
    return src


def extract_clean_articles(df: DataFrame) -> Optional[DataFrame]:
    meta_df = (df
               .withColumn("scraped_at_str", F.col("meta.scraped_at"))
               .withColumn("category", F.col("meta.category"))
               .filter("scraped_at_str is not null and category is not null"))

    if meta_df.isEmpty():
        log.warning("No valid metadata rows")
        return None

    validated_articles = (
        meta_df
        .withColumn("raw_article", F.explode("news"))
        .withColumn(
            "article",
            validate_and_clean_article_udf(F.col("raw_article"), F.col("category"), F.col("scraped_at_str"))
        )
        .filter("article is not null")
        .select(
            "source_file_path",
            "article",
            F.to_timestamp("scraped_at_str").alias("scraped_at_dt")
        )
    )

    return validated_articles


def prepare_final_df(df: DataFrame) -> Optional[DataFrame]:
    if df.isEmpty():
        log.warning("Nothing to write to Iceberg")
        return None

    # NOTE: Assuming the previous fix was applied, the UDF takes `scraped_at_dt`
    # and `file_name`, and correctly handles datetime to string conversion internally.
    records = (df
               .withColumn("iceberg",
                           build_final_article_udf(
                               F.col("article"),
                               F.col("scraped_at_dt"),
                               F.col("source_file_path")
                           ))
               .filter("iceberg is not null"))
    if records.isEmpty():
        log.warning("All rows failed final build")
        return None

    cols = [F.col(f"iceberg.{f.name}").alias(f.name)
            for f in FINNHUB_ARTICLE_SPARK_SCHEMA]
    return records.select(*cols)
