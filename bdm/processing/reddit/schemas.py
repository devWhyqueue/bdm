from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, IntegerType, DoubleType, BooleanType, ArrayType, LongType
)

# JSON Schema for raw Reddit data file validation (Draft-07)
REDDIT_FILE_SCHEMA = {
    "type": "object",
    "required": ["metadata", "posts"],
    "properties": {
        "metadata": {
            "type": "object",
            "required": ["subreddit", "sort_by", "scraped_at", "post_count"],
            "properties": {
                "subreddit": {"type": "string"},
                "sort_by": {"type": "string"},
                "time_filter": {"type": ["string", "null"]},
                "post_count": {"type": "integer", "minimum": 0},
                "scraped_at": {"type": "string", "format": "date-time"}
            }
        },
        "posts": {
            "type": "array",
            "items": {
                "type": "object",
                "required": [
                    "id", "title", "author", "created_utc", "score",
                    "url", "selftext", "num_comments", "permalink",
                    "upvote_ratio", "is_original_content", "is_self"
                ],
                "properties": {
                    "id": {"type": "string", "pattern": "^[A-Za-z0-9]+$"},
                    "title": {"type": "string", "minLength": 1},
                    "author": {"type": "string", "minLength": 1},
                    "created_utc": {"type": "number", "minimum": 0},  # UNIX seconds
                    "score": {"type": "integer"},
                    "url": {"type": "string", "format": "uri"},
                    "selftext": {"type": "string"},
                    "num_comments": {"type": "integer", "minimum": 0},
                    "permalink": {"type": "string"},
                    "upvote_ratio": {"type": "number", "minimum": 0, "maximum": 1},
                    "is_original_content": {"type": "boolean"},
                    "is_self": {"type": "boolean"}
                }
            }
        }
    }
}

# PySpark Schema for the 'reddit_posts' Iceberg table
REDDIT_POST_SPARK_SCHEMA = StructType([
    StructField("id", StringType(), nullable=False),
    StructField("title", StringType(), nullable=False),
    StructField("author", StringType(), nullable=False),
    StructField("created_utc", TimestampType(), nullable=False),  # Converted to UTC Timestamp
    StructField("score", IntegerType(), nullable=False),
    StructField("url", StringType(), nullable=False),
    StructField("selftext", StringType(), nullable=True),
    StructField("num_comments", IntegerType(), nullable=False),
    StructField("permalink", StringType(), nullable=False),
    StructField("upvote_ratio", DoubleType(), nullable=False),
    StructField("is_original_content", BooleanType(), nullable=False),
    StructField("is_self", BooleanType(), nullable=False),
    StructField("subreddit", StringType(), nullable=False),  # From metadata
    StructField("scraped_at", TimestampType(), nullable=False),  # From metadata, for partitioning
    StructField("source_file", StringType(), nullable=False),
    StructField("checksum_sha256", StringType(), nullable=False),
    StructField("media_items", ArrayType(MEDIA_ITEM_SCHEMA, True), nullable=True) # Added media_items field
])

# Define the schema for individual media items as processed and stored
MEDIA_ITEM_SCHEMA = StructType([
    StructField("media_type", StringType(), True), # Original declared media type
    StructField("s3_url", StringType(), True), # Original S3 URL
    StructField("source_url", StringType(), True), # Original source URL from Reddit
    StructField("filename", StringType(), True), # Original filename
    StructField("content_type", StringType(), True), # Original content type
    StructField("processed_s3_url", StringType(), True), # S3 URL after processing
    StructField("processed_format", StringType(), True), # e.g., 'jpeg', 'mp4'
    StructField("processed_size_bytes", LongType(), True), # Size of the processed file
    StructField("validation_error", StringType(), True), # Error message if validation failed
    StructField("conversion_error", StringType(), True), # Error message if conversion failed
    StructField("size_error", StringType(), True) # Error message if size check failed
])

# PySpark Schemas for processing raw data in Spark
# Define the schema for individual media items as found in the raw JSON
RAW_MEDIA_ITEM_SCHEMA_ENTRY = StructType([
    StructField("media_type", StringType(), True),
    StructField("s3_url", StringType(), True),
    StructField("source_url", StringType(), True),
    StructField("filename", StringType(), True),
    StructField("content_type", StringType(), True)
])

SPARK_RAW_POST_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("author", StringType(), True),
    StructField("created_utc", DoubleType(), True),  # Raw data has it as number
    StructField("score", IntegerType(), True),
    StructField("url", StringType(), True),
    StructField("selftext", StringType(), True),
    StructField("num_comments", IntegerType(), True),
    StructField("permalink", StringType(), True),
    StructField("upvote_ratio", DoubleType(), True),
    StructField("is_original_content", BooleanType(), True),
    StructField("is_self", BooleanType(), True),
    StructField("media", ArrayType(RAW_MEDIA_ITEM_SCHEMA_ENTRY, True), True) # Added media field
])

SPARK_RAW_METADATA_SCHEMA = StructType([
    StructField("subreddit", StringType(), True),
    StructField("sort_by", StringType(), True),
    StructField("time_filter", StringType(), True),
    StructField("post_count", IntegerType(), True),
    StructField("scraped_at", StringType(), True),  # Raw data has it as string
])

SPARK_RAW_FILE_SCHEMA = StructType([
    StructField("metadata", SPARK_RAW_METADATA_SCHEMA, True),
    StructField("posts", ArrayType(SPARK_RAW_POST_SCHEMA), True),
])

CLEANED_POST_STRUCT_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("author", StringType(), True),
    StructField("created_utc", TimestampType(), True),  # Cleaned data has it as Timestamp
    StructField("score", IntegerType(), True),
    StructField("num_comments", IntegerType(), True),
    StructField("url", StringType(), True),
    StructField("selftext", StringType(), True),
    StructField("permalink", StringType(), True),
    StructField("upvote_ratio", DoubleType(), True),
    StructField("is_original_content", BooleanType(), True),
    StructField("is_self", BooleanType(), True),
])
