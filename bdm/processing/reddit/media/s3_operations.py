import logging
import os
from typing import Optional, Dict, Any, Tuple

from bdm.utils import get_minio_client

logger = logging.getLogger(__name__)

MINIO_LANDING_BUCKET = os.environ.get("MINIO_LANDING_BUCKET")
MINIO_TRUSTED_BUCKET = os.environ.get("MINIO_TRUSTED_BUCKET")

s3_client = get_minio_client()


def is_s3_configured() -> bool:
    """Checks if S3 client and buckets are configured."""
    if not s3_client:
        logger.error("S3 client not initialized.")
        return False
    if not MINIO_LANDING_BUCKET:
        logger.error("MINIO_LANDING_BUCKET not configured.")
        return False
    if not MINIO_TRUSTED_BUCKET:
        logger.error("MINIO_TRUSTED_BUCKET not configured.")
        return False
    return True


def get_s3_object_details(object_key: str) -> Optional[Dict[str, Any]]:
    """Gets metadata for an S3 object from the landing bucket."""
    if not s3_client or not MINIO_LANDING_BUCKET:
        logger.error("S3 client or landing bucket not initialized. Cannot get object details.")
        return None
    try:
        response = s3_client.head_object(Bucket=MINIO_LANDING_BUCKET, Key=object_key)
        logger.info(f"Got head_object for s3://{MINIO_LANDING_BUCKET}/{object_key}")
        return {"ContentLength": response.get("ContentLength"), "ContentType": response.get("ContentType")}
    except Exception as e:
        logger.error(f"Failed to get metadata: s3://{MINIO_LANDING_BUCKET}/{object_key}: {e}", exc_info=True)
        return None


def download_s3_file(object_key: str, local_temp_path: str) -> bool:
    """Downloads a file from the S3 landing bucket."""
    if not s3_client or not MINIO_LANDING_BUCKET:
        logger.error("S3 client or landing bucket not initialized. Cannot download file.")
        return False
    try:
        s3_client.download_file(MINIO_LANDING_BUCKET, object_key, local_temp_path)
        logger.info(f"Downloaded s3://{MINIO_LANDING_BUCKET}/{object_key} to {local_temp_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to download s3://{MINIO_LANDING_BUCKET}/{object_key}: {e}", exc_info=True)
        return False


def upload_s3_file(local_file_path: str, object_key: str, content_type: Optional[str] = None) -> bool:
    """Uploads a file to the S3 trusted bucket."""
    if not s3_client or not MINIO_TRUSTED_BUCKET:
        logger.error("S3 client or trusted bucket not initialized. Cannot upload file.")
        return False
    try:
        extra_args = {'ContentType': content_type} if content_type else {}
        s3_client.upload_file(local_file_path, MINIO_TRUSTED_BUCKET, object_key, ExtraArgs=extra_args)
        logger.info(f"Uploaded {local_file_path} to s3://{MINIO_TRUSTED_BUCKET}/{object_key}")
        return True
    except Exception as e:
        logger.error(f"Failed to upload {local_file_path} to s3://{MINIO_TRUSTED_BUCKET}/{object_key}: {e}",
                     exc_info=True)
        return False


def construct_processed_s3_url(original_s3_key_relative: str, new_format_extension: str) -> Tuple[str, str]:
    """Constructs the S3 URL and key for a processed media item in the trusted bucket."""
    base_path, original_filename = os.path.split(original_s3_key_relative)
    base_filename, _ = os.path.splitext(original_filename)
    new_filename = f"{base_filename}_processed.{new_format_extension}"
    processed_key_relative = os.path.join("processed_media", base_path.lstrip('/'), new_filename).replace("\\", "/")

    if not MINIO_TRUSTED_BUCKET:
        logger.critical("MINIO_TRUSTED_BUCKET not configured for URL construction.")
        raise ValueError("MINIO_TRUSTED_BUCKET is not configured.")
    full_s3_url = f"s3a://{MINIO_TRUSTED_BUCKET}/{processed_key_relative}"
    return full_s3_url, processed_key_relative
