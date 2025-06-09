import logging
import os

import boto3
from botocore.config import Config
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def get_minio_client() -> boto3.client:
    """Initialize and return a MinIO S3 client."""
    endpoint = os.environ.get('MINIO_ENDPOINT')
    access_key = os.environ.get('MINIO_ACCESS_KEY')
    secret_key = os.environ.get('MINIO_SECRET_KEY')

    if not endpoint or not access_key or not secret_key:
        raise ValueError("MinIO credentials not found in environment variables")

    return boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

def create_spark_session(app_name: str = "BDMSparkApp") -> SparkSession:
    """Creates and returns a Spark session configured for Iceberg."""
    spark = SparkSession.builder.appName(app_name) \
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://trusted-zone/iceberg_catalog/") \
        .getOrCreate()
    logger.info(f"Spark session '{app_name}' created with Iceberg support.")
    return spark
