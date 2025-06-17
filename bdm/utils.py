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


def create_spark_session(app_name: str = "BDMSparkApp", packages: list[str] = None) -> SparkSession:
    """
    Creates and returns a Spark session configured for Iceberg.
    """
    builder = SparkSession.builder.appName(app_name) \
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://trusted-zone/iceberg_catalog/") \
        .config("spark.openmetadata.transport.pipelineServiceName", app_name) \
        .config("spark.openmetadata.transport.pipelineName", app_name) \
        .config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener") \
        .config("spark.openmetadata.transport.hostPort", "http://openmetadata-server:8585") \
        .config("spark.openmetadata.transport.type", "openmetadata") \
        .config("spark.openmetadata.transport.jwtToken",
                "eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImluZ2VzdGlvbi1ib3QiLCJyb2xlcyI6WyJJbmdlc3Rpb25Cb3RSb2xlIl0sImVtYWlsIjoiaW5nZXN0aW9uLWJvdEBvcGVuLW1ldGFkYXRhLm9yZyIsImlzQm90Ijp0cnVlLCJ0b2tlblR5cGUiOiJCT1QiLCJpYXQiOjE3NTAwNzQzNjEsImV4cCI6bnVsbH0.vAcI2PGDncU1U0c-X4SAp6dkahNU-WJFfs6--rDNIxrT2hTsgi6MINrxhFS_0asvU5NFAhD_IQ8hlVyAu-M42yyBEVnhcz3WENzzqNciOtOujC6zBuhrw8F405kBKjS-cvZlS2gguddue8cJbrielii_IfnyNpfYDTBymAi5tIdKkGV2PL8XsuwYXHIbdzoBJDmU47dkpt3QBE7KRy_xw-gcz-ed8kh6pyasWOxVhe-tcipoYK51BoVvAV25mcqvBcWe7PfAh5QWRgxfM7vSO2VbWiYJCG5Nf_waq2aGFvJosApqnaQ10O2wpjZMpAjRuRuQs9qy8Yhspup_NL5JQA")

    if packages:
        builder = builder.config("spark.jars.packages", ",".join(packages))

    spark = builder.getOrCreate()
    logger.info(f"Spark session '{app_name}' created with Iceberg support.")
    return spark
