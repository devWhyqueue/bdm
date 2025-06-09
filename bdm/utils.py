import os

import boto3
from botocore.config import Config


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


# Import SparkSession here, assuming it's available in the environment
# where this utility function will be called from (e.g., Spark Docker container)
import logging
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

def create_spark_session(app_name: str = "BDMSparkApp") -> SparkSession:
    '''Creates and returns a Spark session configured for Iceberg.'''
    # Note: The .master("local[*]") is useful for local development but should typically
    # not be set here for applications intended to run on a cluster, as it would
    # override cluster manager settings (YARN, Kubernetes).
    # For Dockerized Spark applications submitted with spark-submit, the master URL
    # is usually provided in the spark-submit command or defaults.
    spark = SparkSession.builder.appName(app_name) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "hadoop") \
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://trusted-zone/iceberg_catalog/") \
        .getOrCreate()
    logger.info(f"Spark session '{app_name}' created with Iceberg support.")
    return spark
