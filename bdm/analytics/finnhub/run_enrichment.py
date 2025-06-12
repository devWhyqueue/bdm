import logging
import os

import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StringType, FloatType, StructType, StructField

# Import for transformers pipelines
try:
    from transformers import pipeline as hf_pipeline
except ImportError:
    hf_pipeline = None

from bdm.utils import create_spark_session  # As per user feedback

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

# ─────────────────── Summarisation UDF ────────────────────
_summariser_pipeline = None


def _get_summariser_pipeline():
    """Lazily create or return a singleton summariser pipeline inside each executor."""
    global _summariser_pipeline
    if _summariser_pipeline is None:
        if hf_pipeline is None:
            log.error("Transformers library not available. Cannot create summariser.")
            raise RuntimeError("Transformers library not available.")
        log.info(
            "Initialising summarisation pipeline (sshleifer/distilbart-cnn-12-6) on executor…"
        )
        _summariser_pipeline = hf_pipeline(
            "summarization",
            model="sshleifer/distilbart-cnn-12-6",
            tokenizer="sshleifer/distilbart-cnn-12-6",
        )
        log.info("Summarisation pipeline initialised on executor.")
    return _summariser_pipeline


def _summarise(text: str, min_len: int = 50, max_len: int = 150) -> str:
    if not text or not isinstance(text, str) or len(text.strip()) < 10:
        return text
    try:
        pipe = _get_summariser_pipeline()
        truncated = text[:3000]
        result = pipe(truncated, min_length=min_len, max_length=max_len, truncation=True)
        if result and isinstance(result, list) and "summary_text" in result[0]:
            return result[0]["summary_text"]
        return text
    except Exception as e:  # noqa: BLE001
        log.warning("Summarisation failed; returning original text. Error: %s", e)
        return text


summarise_udf = F.udf(_summarise, StringType())

# ─────────────────── Sentiment Analysis UDF ────────────────────
_sentiment_pipeline = None


def _get_sentiment_pipeline():
    """Lazily create or return a singleton sentiment pipeline inside each executor."""
    global _sentiment_pipeline
    if _sentiment_pipeline is None:
        if hf_pipeline is None:
            log.error("Transformers library not available. Cannot create sentiment pipeline.")
            raise RuntimeError("Transformers library not available.")
        log.info("Initialising Hugging Face sentiment‑analysis pipeline on executor…")
        _sentiment_pipeline = hf_pipeline("sentiment-analysis")
        log.info("Sentiment‑analysis pipeline initialised on executor.")
    return _sentiment_pipeline


def _analyse_sentiment(headline: str, summary: str) -> tuple[float, str]:
    headline = str(headline) if headline is not None else ""
    summary = str(summary) if summary is not None else ""
    text = f"{headline}. {summary}".strip()
    if not text:
        return 0.0, "neutral"
    try:
        pipe = _get_sentiment_pipeline()
        truncated = text[:512]
        res = pipe(truncated)
        if res and isinstance(res, list):
            label = res[0].get("label", "NEUTRAL").lower()
            score = float(res[0].get("score", 0.0))
            if label not in ("positive", "negative"):
                label = "neutral"
            return score, label
        return 0.0, "neutral"
    except Exception as e:  # noqa: BLE001
        log.warning("Sentiment analysis failed; returning neutral. Error: %s", e)
        return 0.0, "neutral"


sentiment_schema = StructType(
    [
        StructField("sentiment_score", FloatType(), False),
        StructField("sentiment_label", StringType(), False),
    ]
)
sentiment_udf = F.udf(_analyse_sentiment, sentiment_schema)

# ─────────────────── Main Transformation Logic ────────────────────

def enrich_and_transform_articles(spark: SparkSession, df: DataFrame) -> DataFrame:
    """Apply summarisation & sentiment analysis, prune columns, and rename fields."""
    log.info("Starting enrichment and transformation…")

    # 1️⃣  Summarisation – fill or replace the *summary* column **in‑place**
    df_enriched = df.withColumn(
        "summary",
        F.when(
            (F.col("summary").isNull()) | (F.length("summary") < 50),
            summarise_udf(F.coalesce(F.col("headline"), F.lit("No content to summarise"))),
        ).otherwise(F.col("summary")),
    )

    # 2️⃣  Sentiment analysis – create two new columns
    df_sentiment = df_enriched.withColumn(
        "sentiment_struct", sentiment_udf(F.col("headline"), F.col("summary"))
    ).withColumn(
        "sentiment_score", F.col("sentiment_struct.sentiment_score")
    ).withColumn(
        "sentiment_label", F.col("sentiment_struct.sentiment_label")
    ).drop("sentiment_struct")

    # 3️⃣  Prune and rename
    df_final = (
        df_sentiment.withColumnRenamed("published_at_utc", "published_at")
        .drop("source_file", "checksum_sha256")
    )

    final_columns = [
        "id",
        "category",
        "published_at",
        "headline",
        "source",
        "summary",
        "url",
        "image_url",
        "scraped_at",
        "sentiment_score",
        "sentiment_label",
    ]
    return df_final.select(final_columns)


# ─────────────────── Spark Job ────────────────────

def run_spark_job(spark: SparkSession) -> None:
    log.info("Reading from Iceberg table: %s", ICEBERG_TABLE_NAME)
    df_silver = spark.sql(f"SELECT * FROM {ICEBERG_TABLE_NAME}")
    initial_count = df_silver.count()
    log.info("Initial row count from Silver: %s", initial_count)

    df_gold = enrich_and_transform_articles(spark, df_silver)

    final_count = df_gold.count()
    log.info("Row count for Gold (before writing): %s", final_count)

    log.info("Writing transformed data to PostgreSQL table: %s", POSTGRES_TABLE_NAME)
    df_gold.write.jdbc(
        url=PG_JDBC_URL,
        table=POSTGRES_TABLE_NAME,
        mode="overwrite",
        properties=PG_PROPERTIES,
    )


# ─────────────────── Main ────────────────────
if __name__ == "__main__":
    log.info("Starting Finnhub Analytics Spark Job…")
    try:
        if hf_pipeline is None:
            log.critical(
                "Hugging Face Transformers library is not installed. Pipelines are not available."
            )
            raise ImportError("Transformers library not found.")

        spark_session = create_spark_session(app_name="FinnhubAnalyticsEnrichment")
        run_spark_job(spark_session)
        log.info("Finnhub Analytics Spark Job completed successfully.")
    except Exception as e:  # noqa: BLE001
        log.fatal("Fatal error in Finnhub Analytics Spark Job: %s", e, exc_info=True)
    finally:
        if "spark_session" in locals() and spark_session:
            log.info("Stopping Spark session.")
            spark_session.stop()
