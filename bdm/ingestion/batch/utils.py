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
