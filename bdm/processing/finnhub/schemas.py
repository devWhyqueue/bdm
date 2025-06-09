from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, IntegerType, ArrayType, LongType
)

# JSON Schema for raw Finnhub data file validation (Draft-07)
FINNHUB_FILE_SCHEMA = {
    "type": "object",
    "required": ["metadata", "news"],
    "properties": {
        "metadata": {
            "type": "object",
            "required": ["category", "article_count", "scraped_at"],
            "properties": {
                "category":      { "type": "string" },
                "article_count": { "type": "integer", "minimum": 0 },
                "scraped_at":    { "type": "string", "format": "date-time" }
            }
        },
        "news": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["id", "category", "datetime", "headline", "source", "url"],
                "properties": {
                    "id":       { "type": "integer", "minimum": 0 },
                    "category": { "type": "string" },
                    "datetime": { "type": "integer", "minimum": 0 }, # UNIX seconds
                    "headline": { "type": "string", "minLength": 1 },
                    "source":   { "type": "string", "minLength": 1 },
                    "summary":  { "type": "string" },
                    "url":      { "type": "string", "format": "uri" },
                    "image":    { "type": "string", "format": "uri" }
                }
            }
        }
    }
}

# PySpark Schema for the 'finnhub_articles' Iceberg table
FINNHUB_ARTICLE_SPARK_SCHEMA = StructType([
    StructField("article_id", LongType(), nullable=False), # Mapped from id
    StructField("category", StringType(), nullable=False),
    StructField("datetime_utc", TimestampType(), nullable=False), # Converted from datetime
    StructField("headline", StringType(), nullable=False),
    StructField("source", StringType(), nullable=False),
    StructField("summary", StringType(), nullable=True),
    StructField("url", StringType(), nullable=False),
    StructField("image_url", StringType(), nullable=True), # Mapped from image
    StructField("scraped_at", TimestampType(), nullable=False), # From metadata, for partitioning
    StructField("source_file", StringType(), nullable=False),
    StructField("checksum_sha256", StringType(), nullable=False),
])

# PySpark Schemas for processing raw data in Spark
SPARK_RAW_ARTICLE_SCHEMA = StructType([
    StructField("id", LongType(), True),
    StructField("category", StringType(), True),
    StructField("datetime", LongType(), True),  # Raw data has it as integer (Unix seconds)
    StructField("headline", StringType(), True),
    StructField("source", StringType(), True),
    StructField("summary", StringType(), True),
    StructField("url", StringType(), True),
    StructField("image", StringType(), True),
])

SPARK_RAW_METADATA_SCHEMA = StructType([
    StructField("category", StringType(), True),
    StructField("article_count", IntegerType(), True),
    StructField("scraped_at", StringType(), True),  # Raw data has it as string (date-time)
])

SPARK_RAW_FINNHUB_FILE_SCHEMA = StructType([
    StructField("metadata", SPARK_RAW_METADATA_SCHEMA, True),
    StructField("news", ArrayType(SPARK_RAW_ARTICLE_SCHEMA), True),
])

# Schema for the cleaned article before checksum calculation
CLEANED_ARTICLE_STRUCT_SCHEMA = StructType([
    StructField("article_id", LongType(), True),
    StructField("category", StringType(), True),
    StructField("datetime_utc", TimestampType(), True), # Cleaned data has it as Timestamp
    StructField("headline", StringType(), True),
    StructField("source", StringType(), True),
    StructField("summary", StringType(), True),
    StructField("url", StringType(), True),
    StructField("image_url", StringType(), True),
])
