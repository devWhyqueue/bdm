import logging
import os
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, FloatType, StructType, StructField

# Import for summarization (conditionally if transformers is available)
try:
    from transformers import pipeline as hf_pipeline
except ImportError:
    hf_pipeline = None

# Import for sentiment (conditionally if vaderSentiment is available)
try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
except ImportError:
    SentimentIntensityAnalyzer = None

from bdm.utils import create_spark_session # As per user feedback

# ─────────────────── Logging ────────────────────
logging.basicConfig(format="%(asctime)s %(levelname)s %(name)s %(message)s", level=logging.INFO)
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
    "driver": "org.postgresql.Driver"
}

# ─────────────────── Summarization UDF ────────────────────
summarizer_pipeline_instance = None
def get_summarizer_pipeline():
    global summarizer_pipeline_instance
    if summarizer_pipeline_instance is None:
        if hf_pipeline is None:
            log.error("Transformers library not available. Cannot create summarizer.")
            raise RuntimeError("Transformers library not available. Cannot create summarizer.")
        log.info("Initializing summarization pipeline (sshleifer/distilbart-cnn-12-6) on executor...")
        summarizer_pipeline_instance = hf_pipeline(
            "summarization", model="sshleifer/distilbart-cnn-12-6", tokenizer="sshleifer/distilbart-cnn-12-6"
        )
        log.info("Summarization pipeline initialized on executor.")
    return summarizer_pipeline_instance

def summarize_text_udf_logic(text_to_summarize: str, min_length: int = 50, max_length: int = 150) -> str:
    if not text_to_summarize or not isinstance(text_to_summarize, str) or len(text_to_summarize.strip()) < 10:
        return text_to_summarize
    try:
        pipeline = get_summarizer_pipeline()
        max_input_chars = 3000
        truncated_text = text_to_summarize[:max_input_chars]
        summary_list = pipeline(truncated_text, min_length=min_length, max_length=max_length, truncation=True)
        if summary_list and isinstance(summary_list, list) and 'summary_text' in summary_list[0]:
            return summary_list[0]['summary_text']
        return text_to_summarize
    except Exception as e:
        # print(f"Summarization error: {e}")
        return text_to_summarize

summarize_udf = F.udf(summarize_text_udf_logic, StringType())

# ─────────────────── Sentiment Analysis UDF ────────────────────
vader_analyzer_instance = None
def get_vader_analyzer():
    global vader_analyzer_instance
    if vader_analyzer_instance is None:
        if SentimentIntensityAnalyzer is None:
            log.error("vaderSentiment library not available. Cannot create analyzer.")
            raise RuntimeError("vaderSentiment library not available. Cannot create analyzer.")
        log.info("Initializing VADER SentimentIntensityAnalyzer on executor...")
        vader_analyzer_instance = SentimentIntensityAnalyzer()
        log.info("VADER SentimentIntensityAnalyzer initialized on executor.")
    return vader_analyzer_instance

def sentiment_analysis_udf_logic(headline: str, summary: str) -> tuple:
    headline = str(headline) if headline is not None else ""
    summary = str(summary) if summary is not None else ""
    text_for_sentiment = headline + ". " + summary
    if not text_for_sentiment.strip():
        return 0.0, "neutral"
    try:
        analyzer = get_vader_analyzer()
        vs = analyzer.polarity_scores(text_for_sentiment)
        score = vs['compound']
        label = "neutral"
        if score >= 0.05:
            label = "positive"
        elif score <= -0.05:
            label = "negative"
        return float(score), label
    except Exception as e:
        # print(f"Sentiment analysis error: {e}")
        return 0.0, "neutral"

sentiment_schema = StructType([
    StructField("sentiment_score", FloatType(), False),
    StructField("sentiment_label", StringType(), False)
])
sentiment_udf = F.udf(sentiment_analysis_udf_logic, sentiment_schema)

# ─────────────────── Main Transformation Logic ────────────────────
def enrich_and_transform_articles(spark: SparkSession, df: DataFrame) -> DataFrame:
    log.info("Starting enrichment and transformation...")
    log.info("Applying summarization for articles with short/missing summaries...")
    df_enriched = df.withColumn(
        "generated_summary",
        F.when(
            (F.col("summary").isNull()) | (F.length(F.col("summary")) < 50),
            summarize_udf(F.coalesce(F.col("headline"), F.lit("No content to summarize")))
        ).otherwise(F.col("summary"))
    )
    df_enriched = df_enriched.withColumn("summary", F.col("generated_summary")).drop("generated_summary")
    log.info("Summarization step complete.")

    log.info("Applying sentiment analysis...")
    df_sentiment = df_enriched.withColumn(
        "sentiment_struct", sentiment_udf(F.col("headline"), F.col("summary"))
    )
    df_sentiment = df_sentiment.withColumn("sentiment_score", F.col("sentiment_struct.sentiment_score")) \
                               .withColumn("sentiment_label", F.col("sentiment_struct.sentiment_label")) \
                               .drop("sentiment_struct")
    log.info("Sentiment analysis step complete.")

    log.info("Applying schema pruning and renaming...")
    df_final = df_sentiment.withColumnRenamed("published_at_utc", "published_at") \
                           .drop("source_file", "checksum_sha256")
    log.info("Schema pruning and renaming complete.")

    final_columns = [
        "id", "category", "published_at", "headline", "source", "summary", "url", "image_url",
        "scraped_at", "sentiment_score", "sentiment_label"
    ]
    df_final = df_final.select(final_columns)
    return df_final

# ─────────────────── Spark Job ────────────────────
def run_spark_job(spark: SparkSession):
    log.info(f"Reading from Iceberg table: {ICEBERG_TABLE_NAME}")
    try:
        df_silver = spark.read.table(ICEBERG_TABLE_NAME)
    except Exception as e:
        log.error(f"Failed to read from Iceberg table {ICEBERG_TABLE_NAME}: {e}", exc_info=True)
        raise

    log.info("Source schema:")
    df_silver.printSchema()
    initial_count = df_silver.count()
    log.info(f"Initial row count from Silver: {initial_count}")
    if initial_count == 0:
        log.warning("Source table is empty. Proceeding, but no data will be processed.")

    df_gold = enrich_and_transform_articles(spark, df_silver)

    log.info("Gold schema:")
    df_gold.printSchema()
    final_count = df_gold.count()
    log.info(f"Row count for Gold (before writing): {final_count}")

    log.info(f"Writing transformed data to PostgreSQL table: {POSTGRES_TABLE_NAME} at {PG_JDBC_URL}")
    try:
        (df_gold.write
            .format("jdbc")
            .option("url", PG_JDBC_URL)
            .option("dbtable", POSTGRES_TABLE_NAME)
            .option("user", PG_USER)
            .option("password", PG_PASSWORD)
            .option("driver", PG_PROPERTIES["driver"])
            .mode("overwrite")
            .save())
        log.info("Successfully wrote data to PostgreSQL.")
    except Exception as e:
        log.error(f"Failed to write to PostgreSQL table {POSTGRES_TABLE_NAME}: {e}", exc_info=True)
        raise

# ─────────────────── Main ────────────────────
if __name__ == "__main__":
    log.info("Starting Finnhub Analytics Spark Job...")
    spark_session = None
    try:
        if hf_pipeline is None:
            log.critical("Hugging Face Transformers library is not installed. Summarization is not possible.")
            raise ImportError("Transformers library not found.")
        if SentimentIntensityAnalyzer is None:
            log.critical("vaderSentiment library is not installed. Sentiment analysis is not possible.")
            raise ImportError("vaderSentiment library not found.")

        spark_session = create_spark_session(app_name="FinnhubAnalyticsEnrichment")

        try:
            import nltk
            nltk.data.find('tokenizers/punkt')
        except ImportError:
            log.warning("NLTK library not found. VADER may not perform optimally or fail if 'punkt' is needed and not found.")
        except nltk.downloader.DownloadError:
            log.warning("NLTK 'punkt' tokenizer not found. VADER may not perform optimally. Consider `nltk.download('punkt')` in Dockerfile.")
        except LookupError:
            log.warning("NLTK 'punkt' tokenizer not found by nltk.data.find. VADER may not perform optimally.")

        run_spark_job(spark_session)
        log.info("Finnhub Analytics Spark Job completed successfully.")
    except ImportError as ie:
        log.fatal(f"Import error: {ie}")
    except Exception as e:
        log.fatal(f"Fatal error in Finnhub Analytics Spark Job: {e}", exc_info=True)
    finally:
        if spark_session:
            log.info("Stopping Spark session.")
            spark_session.stop()
