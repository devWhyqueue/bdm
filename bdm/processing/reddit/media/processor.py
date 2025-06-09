import logging
import os
import tempfile
from typing import Optional, Dict, Any, Tuple

from bdm.processing.reddit.media.conversion_utils import (
    get_true_mime_type,
    convert_image_to_jpeg,
    convert_to_h264_mp4,
    construct_processed_s3_url
)
from bdm.processing.reddit.media.s3_operations import (
    download_s3_file,
    upload_s3_file,
    MINIO_LANDING_BUCKET,
    MINIO_TRUSTED_BUCKET
)

logger = logging.getLogger(__name__)

MAX_FILE_SIZE_BYTES = 50 * 1024 * 1024


def _init_output_item(media_item: Dict[str, Any]) -> Dict[str, Any]:
    output = media_item.copy()
    output.update({
        "tz_url": None, "tz_format": None, "tz_size_bytes": None,
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
    original_s3_key_relative = media_item.get("lz_url")
    if not original_s3_key_relative:
        return None, None, None, None, "Missing s3_url in media_item"

    declared_media_type = media_item.get("media_type", "").lower()
    original_filename = media_item.get("filename", os.path.basename(original_s3_key_relative) or "temp_media_file")

    temp_dir = tempfile.mkdtemp(prefix="media_proc_")
    local_input_path = os.path.join(temp_dir, original_filename)
    logger.info(f"Processing {original_s3_key_relative} from {source_file_name_for_logging} in {temp_dir}")

    if not MINIO_LANDING_BUCKET:
        _cleanup_temp_dir(temp_dir)
        return None, None, None, None, "MINIO_LANDING_BUCKET not configured."
    if not download_s3_file(original_s3_key_relative, local_input_path):
        _cleanup_temp_dir(temp_dir)
        return None, None, None, None, f"Failed to download from S3: s3://{MINIO_LANDING_BUCKET}/{original_s3_key_relative}"

    true_mime_type = get_true_mime_type(local_input_path)
    if not true_mime_type:
        _cleanup_temp_dir(temp_dir)
        return None, None, None, None, "Could not determine true MIME type"

    valid_type = (declared_media_type == "image" and true_mime_type.startswith(
        "image/") and true_mime_type != "image/gif") or \
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
    base_filename_no_ext, _ = os.path.splitext(original_filename)

    if declared_media_type == "image":
        target_format, target_content_type = "jpeg", "image/jpeg"
        conversion_func_ptr = convert_image_to_jpeg
    elif declared_media_type in ["gif", "video"]:
        target_format, target_content_type = "mp4", "video/mp4"
        conversion_func_ptr = convert_to_h264_mp4
    else:
        return None, None, None, f"Unsupported media type for conversion: {declared_media_type}"

    local_output_path = os.path.join(temp_dir, f"{base_filename_no_ext}_processed.{target_format}")
    if not conversion_func_ptr(local_input_path, local_output_path):
        err_msg = f"{declared_media_type.upper()} to {target_format.upper()} conversion failed"
        return None, None, None, err_msg

    return local_output_path, target_format, target_content_type, None


def _upload_and_verify_processed_media(local_output_path: str, original_s3_key_relative: str,
                                       target_format: str, target_content_type: str) \
        -> Tuple[Optional[str], Optional[int], Optional[str]]:
    try:
        full_s3_url, processed_key_relative = construct_processed_s3_url(original_s3_key_relative, target_format)
    except ValueError as e:
        return None, None, str(e)

    if not MINIO_TRUSTED_BUCKET:
        return None, None, "MINIO_TRUSTED_BUCKET not configured for upload."
    if not upload_s3_file(local_output_path, processed_key_relative, target_content_type):
        return None, None, f"Failed to upload to S3: s3://{MINIO_TRUSTED_BUCKET}/{processed_key_relative}"

    tz_size_bytes = os.path.getsize(local_output_path)
    if tz_size_bytes > MAX_FILE_SIZE_BYTES:
        err_msg = f"Processed file {processed_key_relative} ({tz_size_bytes} B) exceeds max size ({MAX_FILE_SIZE_BYTES} B)"
        return full_s3_url, tz_size_bytes, err_msg

    return full_s3_url, tz_size_bytes, None


# --- Main Processing Function ---

def _handle_initial_s3_check(output_item: Dict[str, Any]) -> bool:
    if not MINIO_LANDING_BUCKET:
        output_item["validation_error"] = "MINIO_LANDING_BUCKET not configured."
        return False
    if not MINIO_TRUSTED_BUCKET:
        output_item["validation_error"] = "MINIO_TRUSTED_BUCKET not configured."
        return False
    return True


def _run_validation_and_download(
        output_item: Dict[str, Any], media_item: Dict[str, Any], source_file_name_for_logging: str
) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """Validates, downloads media. Updates output_item with validation_error if any.
    
    Returns:
        Tuple containing local_input_path, declared_type, temp_dir, and s3_url_relative.
    """
    s3_url_relative = media_item.get("lz_url")
    local_input_path, _, declared_type, temp_dir, err_msg = \
        _validate_and_download_media(media_item, source_file_name_for_logging)

    if err_msg:
        output_item["validation_error"] = err_msg
        return None, None, temp_dir, s3_url_relative
    return local_input_path, declared_type, temp_dir, s3_url_relative


def _run_conversion(
        output_item: Dict[str, Any], local_input_path: str, declared_type: str,
        original_filename: str, temp_dir: str
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Converts media. Updates output_item with conversion_error if any.
    
    Returns:
        Tuple containing local_output_path, target_format, and target_content_type.
    """
    local_output_path, target_format, target_content_type, err_msg = \
        _convert_media_item(local_input_path, declared_type, original_filename, temp_dir)

    if err_msg:
        output_item["conversion_error"] = err_msg
        return None, None, None
    return local_output_path, target_format, target_content_type


def _run_upload_and_verification(
        output_item: Dict[str, Any], local_output_path: str, s3_url_relative: str,
        target_format: str, target_content_type: str
) -> None:
    """Uploads, verifies size. Updates output_item with results/errors."""
    tz_url, size_bytes, err_msg = \
        _upload_and_verify_processed_media(local_output_path, s3_url_relative, target_format, target_content_type)

    output_item["tz_url"] = tz_url
    output_item["tz_format"] = target_format
    output_item["tz_size_bytes"] = size_bytes if size_bytes is not None else 0

    if err_msg:
        if "size" in err_msg.lower() and "exceeds limit" in err_msg.lower():
            output_item["size_error"] = err_msg
            output_item["tz_url"] = None
            output_item["tz_format"] = None
        elif "Could not determine size" in err_msg:
            output_item["size_error"] = err_msg
        else:
            output_item["conversion_error"] = err_msg


def process_single_media_item(media_item: Dict[str, Any], source_file_name_for_logging: str) -> Dict[str, Any]:
    """Processes a single media item: download, validate, convert, upload, and size check."""
    output_item = _init_output_item(media_item)
    temp_dir: Optional[str] = None

    try:
        if not _handle_initial_s3_check(output_item): return output_item

        s3_url_orig = media_item.get("lz_url")
        original_filename = media_item.get("filename", os.path.basename(s3_url_orig or "") or "temp_media_file")

        local_input_path, declared_type, temp_dir_val, s3_url_rel = \
            _run_validation_and_download(output_item, media_item, source_file_name_for_logging)
        temp_dir = temp_dir_val
        if output_item.get("validation_error"): return output_item
        assert local_input_path and declared_type and temp_dir and s3_url_rel, "Validation step failed unexpectedly"

        local_output_path, target_format, target_content_type = \
            _run_conversion(output_item, local_input_path, declared_type, original_filename, temp_dir)
        if output_item.get("conversion_error"): return output_item
        assert local_output_path and target_format and target_content_type, "Conversion step failed unexpectedly"

        _run_upload_and_verification(output_item, local_output_path, s3_url_rel, target_format, target_content_type)

    except Exception as e:
        logger.error(f"Generic error in process_single_media_item for {media_item.get('lz_url')}: {e}", exc_info=True)
        if not any(output_item.get(err) for err in ["validation_error", "conversion_error", "size_error"]):
            output_item["conversion_error"] = f"Unexpected error: {str(e)}"
    finally:
        if temp_dir: _cleanup_temp_dir(temp_dir)

    return output_item
