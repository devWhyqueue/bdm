import logging
import os
import subprocess
import tempfile
from typing import Optional, Dict, Any, Tuple # Added Tuple
import boto3
import magic

logger = logging.getLogger(__name__)

# Global S3 client and configuration constants
S3_ENDPOINT_URL = os.environ.get("MINIO_ENDPOINT")
S3_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY")
S3_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY")
S3_LANDING_BUCKET = os.environ.get("LANDING_ZONE_BUCKET")
S3_PROCESSED_BUCKET = os.environ.get("PROCESSED_ZONE_BUCKET")
MAX_FILE_SIZE_BYTES = 50 * 1024 * 1024  # 50 MB

s3_client = None
if S3_ENDPOINT_URL and S3_ACCESS_KEY and S3_SECRET_KEY and S3_LANDING_BUCKET and S3_PROCESSED_BUCKET:
    try:
        s3_client = boto3.client(
            "s3",
            endpoint_url=S3_ENDPOINT_URL,
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY
        )
        logger.info(f"Successfully initialized S3 client for endpoint: {S3_ENDPOINT_URL}")
    except Exception as e:
        logger.error(f"Failed to initialize S3 client: {e}", exc_info=True)
        s3_client = None
else:
    logger.warning(
        "S3 client environment variables (MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, "
        "LANDING_ZONE_BUCKET, PROCESSED_ZONE_BUCKET) not fully set. S3 operations will likely fail."
    )

# --- S3 and System Utility Functions ---

def get_s3_object_details(bucket_name: str, object_key: str) -> Optional[Dict[str, Any]]:
    if not s3_client:
        logger.error("S3 client not initialized. Cannot get object details.")
        return None
    try:
        response = s3_client.head_object(Bucket=bucket_name, Key=object_key)
        logger.info(f"Got head_object for s3://{bucket_name}/{object_key}")
        return {"ContentLength": response.get("ContentLength"), "ContentType": response.get("ContentType")}
    except Exception as e:
        logger.error(f"Failed to get metadata: s3://{bucket_name}/{object_key}: {e}", exc_info=True)
        return None

def download_s3_file(bucket_name: str, object_key: str, local_temp_path: str) -> bool:
    if not s3_client:
        logger.error("S3 client not initialized. Cannot download file.")
        return False
    try:
        s3_client.download_file(bucket_name, object_key, local_temp_path)
        logger.info(f"Downloaded s3://{bucket_name}/{object_key} to {local_temp_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to download s3://{bucket_name}/{object_key}: {e}", exc_info=True)
        return False

def upload_s3_file(local_file_path: str, bucket_name: str, object_key: str, content_type: Optional[str] = None) -> bool:
    if not s3_client:
        logger.error("S3 client not initialized. Cannot upload file.")
        return False
    try:
        extra_args = {'ContentType': content_type} if content_type else {}
        s3_client.upload_file(local_file_path, bucket_name, object_key, ExtraArgs=extra_args)
        logger.info(f"Uploaded {local_file_path} to s3://{bucket_name}/{object_key}")
        return True
    except Exception as e:
        logger.error(f"Failed to upload {local_file_path} to s3://{bucket_name}/{object_key}: {e}", exc_info=True)
        return False

def get_true_mime_type(local_file_path: str) -> Optional[str]:
    try:
        mime = magic.from_file(local_file_path, mime=True)
        logger.info(f"True MIME type for {local_file_path}: {mime}")
        return mime
    except Exception as e:
        logger.error(f"Error getting MIME type for {local_file_path}: {e}", exc_info=True)
        return None

def _run_subprocess_command(command: list[str], input_path_for_log: str) -> bool:
    try:
        process = subprocess.run(command, check=True, capture_output=True, text=True)
        logger.info(f"Successfully ran {command[0]} for {input_path_for_log}. STDOUT: {process.stdout}")
        if process.stderr: # FFmpeg often logs to stderr even on success
             logger.info(f"{command[0]} STDERR for {input_path_for_log}: {process.stderr}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"{command[0]} error for {input_path_for_log}. STDERR: {e.stderr}. STDOUT: {e.stdout}")
        return False
    except FileNotFoundError:
        logger.error(f"{command[0]} command not found. Ensure it is installed and in PATH.")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during {command[0]} for {input_path_for_log}: {e}", exc_info=True)
        return False

def convert_image_to_jpeg(input_path: str, output_path: str) -> bool:
    return _run_subprocess_command(["convert", input_path, output_path], input_path)

def convert_to_h264_mp4(input_path: str, output_path: str) -> bool:
    command = ["ffmpeg", "-i", input_path, "-vcodec", "libx264", "-acodec", "aac", "-y", output_path]
    return _run_subprocess_command(command, input_path)

def construct_processed_s3_url(original_s3_key_relative: str, new_format_extension: str) -> Tuple[str, str]:
    base_path, original_filename = os.path.split(original_s3_key_relative)
    base_filename, _ = os.path.splitext(original_filename)
    new_filename = f"{base_filename}_processed.{new_format_extension}"
    processed_key_relative = os.path.join("processed_media", base_path, new_filename)

    if not S3_PROCESSED_BUCKET:
        logger.critical("S3_PROCESSED_BUCKET not configured for URL construction.")
        raise ValueError("S3_PROCESSED_BUCKET is not configured.")
    full_s3_url = f"s3a://{S3_PROCESSED_BUCKET}/{processed_key_relative}"
    return full_s3_url, processed_key_relative


# --- Helper Functions for process_single_media_item ---

def _init_output_item(media_item: Dict[str, Any]) -> Dict[str, Any]:
    output = media_item.copy()
    output.update({
        "processed_s3_url": None, "processed_format": None, "processed_size_bytes": None,
        "validation_error": None, "conversion_error": None, "size_error": None,
    })
    return output

def _cleanup_temp_dir(temp_dir: Optional[str]):
    if not temp_dir or not os.path.exists(temp_dir):
        return
    try:
        for item_name in os.listdir(temp_dir):
            item_path = os.path.join(temp_dir, item_name)
            if os.path.isfile(item_path) or os.path.islink(item_path):
                os.unlink(item_path)
        os.rmdir(temp_dir)
        logger.info(f"Successfully cleaned up temp directory: {temp_dir}")
    except Exception as e:
        logger.error(f"Error cleaning up temp directory {temp_dir}: {e}", exc_info=True)

def _validate_and_download_media(media_item: Dict[str, Any], source_file_name_for_logging: str) \
        -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]:
    # Returns: local_input_path, true_mime_type, declared_media_type, temp_dir, error_message_or_None
    original_s3_key_relative = media_item.get("s3_url")
    if not original_s3_key_relative:
        return None, None, None, None, "Missing s3_url in media_item"

    declared_media_type = media_item.get("media_type", "").lower()
    original_filename = media_item.get("filename", os.path.basename(original_s3_key_relative) or "temp_media_file")

    temp_dir = tempfile.mkdtemp(prefix="media_proc_")
    local_input_path = os.path.join(temp_dir, original_filename)
    logger.info(f"Processing {original_s3_key_relative} from {source_file_name_for_logging} in {temp_dir}")

    if not S3_LANDING_BUCKET: # Should be caught by global check, but good for direct calls
        _cleanup_temp_dir(temp_dir)
        return None, None, None, None, "S3_LANDING_BUCKET not configured."
    if not download_s3_file(S3_LANDING_BUCKET, original_s3_key_relative, local_input_path):
        _cleanup_temp_dir(temp_dir)
        return None, None, None, None, f"Failed to download from S3: s3://{S3_LANDING_BUCKET}/{original_s3_key_relative}"

    true_mime_type = get_true_mime_type(local_input_path)
    if not true_mime_type:
        _cleanup_temp_dir(temp_dir)
        return None, None, None, None, "Could not determine true MIME type"

    valid_type = (declared_media_type == "image" and true_mime_type.startswith("image/") and true_mime_type != "image/gif") or \
                 (declared_media_type == "gif" and true_mime_type == "image/gif") or \
                 (declared_media_type == "video" and true_mime_type.startswith("video/"))
    if not valid_type:
        _cleanup_temp_dir(temp_dir)
        err_msg = f"Type '{declared_media_type}' (true: '{true_mime_type}') mismatch or invalid"
        return None, None, None, None, err_msg

    return local_input_path, true_mime_type, declared_media_type, temp_dir, None

def _convert_media_item(local_input_path: str, declared_media_type: str,
                        original_filename: str, temp_dir: str) \
        -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    # Returns: local_output_path, target_format, target_content_type, error_message_or_None
    base_filename_no_ext, _ = os.path.splitext(original_filename)
    target_format, target_content_type, conversion_func = "", "", None

    if declared_media_type == "image":
        target_format, target_content_type = "jpeg", "image/jpeg"
        conversion_func = convert_image_to_jpeg
    elif declared_media_type in ["gif", "video"]:
        target_format, target_content_type = "mp4", "video/mp4"
        conversion_func = convert_to_h264_mp4
    else:
        return None, None, None, f"Unsupported media type for conversion: {declared_media_type}"

    local_output_path = os.path.join(temp_dir, f"{base_filename_no_ext}_processed.{target_format}")
    if not conversion_func(local_input_path, local_output_path):
        err_msg = f"{declared_media_type.upper()} to {target_format.upper()} conversion failed"
        return None, None, None, err_msg

    return local_output_path, target_format, target_content_type, None

def _upload_and_verify_processed_media(local_output_path: str, original_s3_key_relative: str,
                                     target_format: str, target_content_type: str) \
        -> Tuple[Optional[str], Optional[int], Optional[str]]:
    # Returns: processed_s3_full_url, processed_size_bytes, error_message_or_None

    try:
        full_s3_url, processed_key_relative = construct_processed_s3_url(original_s3_key_relative, target_format)
    except ValueError as e: # Raised if S3_PROCESSED_BUCKET is not set
        return None, None, str(e)

    if not S3_PROCESSED_BUCKET: # Should be caught by global or construct_processed_s3_url
         return None, None, "S3_PROCESSED_BUCKET not configured for upload."
    if not upload_s3_file(local_output_path, S3_PROCESSED_BUCKET, processed_key_relative, content_type=target_content_type):
        return None, None, "Failed to upload processed file to S3"

    try:
        processed_file_size = os.path.getsize(local_output_path)
    except OSError as e:
        logger.error(f"Could not get size of {local_output_path}: {e}", exc_info=True)
        return full_s3_url, None, "Could not determine size of processed file" # URL is set, but size error

    if processed_file_size > MAX_FILE_SIZE_BYTES:
        err_msg = f"Processed file size ({processed_file_size} B) exceeds limit ({MAX_FILE_SIZE_BYTES} B)"
        # Note: File is uploaded, but marked as error. Consider deleting from S3 if this is critical.
        return full_s3_url, processed_file_size, err_msg

    return full_s3_url, processed_file_size, None

# --- Main Processing Function ---

def process_single_media_item(media_item: Dict[str, Any], source_file_name_for_logging: str) -> Dict[str, Any]:
    output_item = _init_output_item(media_item)
    temp_dir = None # Ensure temp_dir is always defined for finally block

    try:
        if not s3_client or not S3_LANDING_BUCKET or not S3_PROCESSED_BUCKET:
            output_item["validation_error"] = "S3 client or bucket configuration missing on worker"
            logger.critical("S3 client or bucket env vars not configured. Cannot process media.")
            return output_item

        s3_url_relative = media_item.get("s3_url")
        original_filename = media_item.get("filename", os.path.basename(s3_url_relative or "") or "temp_media_file")

        local_input_path, true_mime, declared_type, temp_dir, err_msg = \
            _validate_and_download_media(media_item, source_file_name_for_logging)
        if err_msg:
            output_item["validation_error"] = err_msg; return output_item

        # These should be set if _validate_and_download_media succeeded
        assert local_input_path and declared_type and temp_dir

        local_output_path, target_format, target_content_type, err_msg = \
            _convert_media_item(local_input_path, declared_type, original_filename, temp_dir)
        if err_msg:
            output_item["conversion_error"] = err_msg; return output_item

        assert local_output_path and target_format and target_content_type and s3_url_relative

        processed_s3_url, size_bytes, err_msg = \
            _upload_and_verify_processed_media(local_output_path, s3_url_relative, target_format, target_content_type)

        # Populate common fields even if there's an upload/size error
        output_item["processed_s3_url"] = processed_s3_url
        output_item["processed_format"] = target_format # Format is known even if upload/size fails
        output_item["processed_size_bytes"] = size_bytes if size_bytes is not None else 0


        if err_msg: # This error is from upload or size check
            # Size error is specific, other errors from _upload_and_verify are upload related (conversion_error bucket)
            if "size" in err_msg.lower() and "exceeds limit" in err_msg.lower() : # More specific check for size limit error
                output_item["size_error"] = err_msg
                output_item["processed_s3_url"] = None # Nullify URL if size error is critical for use
                output_item["processed_format"] = None # Nullify format if size error is critical
            elif "Could not determine size" in err_msg: # Handle case where size could not be determined
                 output_item["size_error"] = err_msg
                 # Keep URL and format as file might be usable, but size is unknown
            else: # Treat as upload error
                output_item["conversion_error"] = err_msg
            return output_item

        # All successful if we reach here without returning
        # output_item already updated with s3_url, format, size from above

    except Exception as e:
        logger.error(f"Generic error in process_single_media_item for {media_item.get('s3_url')}: {e}", exc_info=True)
        # Avoid overwriting specific errors if already set
        if not any(output_item.get(err_field) for err_field in ["validation_error", "conversion_error", "size_error"]):
            output_item["conversion_error"] = f"Unexpected error: {str(e)}"
    finally:
        if temp_dir: # temp_dir would be None if initial s3_url check failed very early
            _cleanup_temp_dir(temp_dir)

    return output_item
