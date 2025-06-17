from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime
from typing import Optional

import pyspark.sql.functions as F
# ─────────────────── Spark NLP 6 ────────────────────
import sparknlp
from pyspark.sql import DataFrame, SparkSession
from sparknlp.annotator import T5Transformer, SentimentDLModel
from sparknlp.base import DocumentAssembler, Finisher, Pipeline

from bdm.utils import create_spark_session  # cluster‑specific helper

# ─────────────────── Logging ────────────────────
logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

# ─────────────────── Constants ────────────────────
ICEBERG_TABLE_NAME = "catalog.finnhub_articles"
POSTGRES_TABLE_NAME = "public.finnhub_articles"

PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DATABASE = os.getenv("POSTGRES_ANALYTICS_DB", "analytics_db")
PG_USER = os.getenv("POSTGRES_ANALYTICS_USER", "analytics_user")
PG_PASSWORD = os.getenv("POSTGRES_ANALYTICS_PASSWORD", "analytics_password")

PG_JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
PG_PROPERTIES = {
    "user": PG_USER,
    "password": PG_PASSWORD,
    "driver": "org.postgresql.Driver",
}


# ─────────────────── CLI ────────────────────

def _parse_args() -> argparse.Namespace:
    """Parse CLI flags."""
    p = argparse.ArgumentParser(description="Finnhub articles enrichment (Spark NLP 6 – T5)")
    p.add_argument("--filter-scraped-at", type=str, help="Filter by scraped_at ISO timestamp")
    return p.parse_args()


# ─────────────────── NLP pipelines ────────────────────

def _build_summariser_pipeline() -> Pipeline:
    """Return a Spark NLP pipeline that generates ``summary_generated``."""
    doc = (
        DocumentAssembler()
        .setInputCol("text_for_summary")
        .setOutputCol("doc_summary")
    )

    summariser = (
        T5Transformer
        .pretrained("t5_base_summary_en", "en")
        .setInputCols("doc_summary")
        .setTask("summarize:")
        .setMinOutputLength(50)
        .setMaxOutputLength(150)
        .setOutputCol("summary_annot")
    )

    finish = (
        Finisher()
        .setInputCols("summary_annot")
        .setOutputCols("summary_generated")
        .setOutputAsArray(True)
        .setCleanAnnotations(False)
    )

    return Pipeline(stages=[doc, summariser, finish])


def _build_sentiment_pipeline() -> Pipeline:
    """Return a Spark NLP pipeline that produces ``sentiment_annot``."""
    doc = (
        DocumentAssembler()
        .setInputCol("sentiment_input")
        .setOutputCol("doc_sentiment")
    )

    sentiment = (
        SentimentDLModel
        .pretrained("sentimentdl_use_twitter", "en")
        .setInputCols("doc_sentiment")
        .setOutputCol("sentiment_annot")
    )

    return Pipeline(stages=[doc, sentiment])


# ─────────────────── Transformation logic ────────────────────

def enrich_and_transform_articles(df: DataFrame) -> DataFrame:
    """Run summarisation, sentiment, and reshape for Gold layer."""

    log.info("Enrichment started…")

    # 1️⃣ Select text requiring a summary
    df = df.withColumn(
        "text_for_summary",
        F.when((F.col("summary").isNull()) | (F.length("summary") < 50), F.col("headline"))
        .otherwise(F.col("summary")),
    )

    # 2️⃣ Summarisation (T5)
    summariser_model = _build_summariser_pipeline().fit(df)
    df = summariser_model.transform(df).withColumn(
        "summary_generated", F.col("summary_generated")[0],
    )

    # 3️⃣ Choose final summary
    df = df.withColumn(
        "summary_final",
        F.when((F.col("summary").isNull()) | (F.length("summary") < 50), F.col("summary_generated"))
        .otherwise(F.col("summary")),
    ).drop("summary_generated", "doc_summary", "summary_annot", "text_for_summary")

    # 4️⃣ Build sentiment input (headline + summary)
    df = df.withColumn(
        "sentiment_input", F.concat_ws(". ", F.col("headline"), F.col("summary_final"))
    )

    # 5️⃣ Sentiment analysis (DL)
    sentiment_model = _build_sentiment_pipeline().fit(df)
    df = sentiment_model.transform(df)

    df = df.withColumn("sentiment_label", F.expr("sentiment_annot[0].result"))
    df = df.withColumn(
        "sentiment_score", F.expr("float(sentiment_annot[0].metadata['confidence'])"),
    ).drop("sentiment_input", "doc_sentiment", "sentiment_annot")

    # 6️⃣ Final schema
    df = (
        df.withColumnRenamed("published_at_utc", "published_at")
        .withColumnRenamed("summary_final", "summary")
        .drop("source_file", "checksum_sha256")
    )

    cols = [
        "id", "category", "published_at", "headline", "source", "summary", "url",
        "image_url", "scraped_at", "sentiment_score", "sentiment_label",
    ]
    return df.select(cols)


# ─────────────────── Spark job driver ────────────────────

def run_spark_job(spark: SparkSession, filter_scraped_at: Optional[str] = None) -> None:
    log.info("Reading Silver table…")

    if filter_scraped_at:
        try:
            datetime.fromisoformat(filter_scraped_at.replace("Z", "+00:00"))
            clause = f"WHERE scraped_at = '{filter_scraped_at}'"
        except ValueError:
            log.warning("Invalid --filter-scraped-at: %s", filter_scraped_at)
            clause = ""
    else:
        clause = ""

    df_silver = spark.sql(f"SELECT * FROM {ICEBERG_TABLE_NAME} {clause}")
    log.info("Silver rows: %s", df_silver.count())

    df_gold = enrich_and_transform_articles(df_silver)
    log.info("Gold rows: %s", df_gold.count())

    log.info("Writing Gold to Postgres…")
    df_gold.write.jdbc(
        url=PG_JDBC_URL,
        table=POSTGRES_TABLE_NAME,
        mode="overwrite",
        properties=PG_PROPERTIES,
    )

# ─────────────────── Main ────────────────────
if __name__ == "__main__":
    log.info("Starting Finnhub Spark Job (Spark NLP 6 – T5)…")

    try:
        spark = create_spark_session(
            app_name="FinnhubAnalyticsEnrichmentSparkNLP6T5",
            packages=["com.johnsnowlabs.nlp:spark-nlp_2.12:6.0.3"]
        ) or sparknlp.start()
        args = _parse_args()
        run_spark_job(spark, args.filter_scraped_at)
        log.info("Job completed successfully.")
    except Exception as e:  # noqa: BLE001
        log.fatal("Fatal error: %s", e, exc_info=True)
    finally:
        if "spark" in locals() and spark:
            spark.stop()
