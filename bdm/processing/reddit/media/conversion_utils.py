import logging
import os
import subprocess
from typing import Optional, Tuple, List

from magic import Magic

logger = logging.getLogger(__name__)

MINIO_TRUSTED_BUCKET = os.environ.get("MINIO_TRUSTED_BUCKET")


def get_true_mime_type(local_file_path: str) -> Optional[str]:
    """Determines the true MIME type of a local file using python-magic."""
    try:
        mime = Magic(mime=True).from_file(local_file_path)
        logger.info(f"True MIME type for {local_file_path}: {mime}")
        return mime
    except Exception as e:
        logger.error(f"Error getting MIME type for {local_file_path}: {e}", exc_info=True)
        return None


def _run_subprocess_command(command: List[str], input_path_for_log: str) -> bool:
    """Runs a subprocess command, logging its output and errors."""
    try:
        process = subprocess.run(command, check=True, capture_output=True, text=True)
        logger.info(f"Successfully ran {command[0]} for {input_path_for_log}. STDOUT: {process.stdout}")
        if process.stderr:
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
    """Converts an image to JPEG format using ImageMagick's convert."""
    return _run_subprocess_command(["convert", input_path, output_path], input_path)


def convert_to_h264_mp4(input_path: str, output_path: str) -> bool:
    """Converts a video or GIF to H.264 MP4 format using ffmpeg."""
    command = ["ffmpeg", "-i", input_path, "-vcodec", "libx264", "-acodec", "aac", "-y", output_path]
    return _run_subprocess_command(command, input_path)


def construct_processed_s3_url(original_s3_key_relative: str, new_format_extension: str) -> Tuple[str, str]:
    """Constructs the S3 URL and key for the processed media file."""
    base_path, original_filename = os.path.split(original_s3_key_relative)
    base_filename, _ = os.path.splitext(original_filename)
    new_filename = f"{base_filename}.{new_format_extension}"

    processed_key_relative = os.path.join("processed_media", base_path, new_filename).replace("\\", "/")

    if not MINIO_TRUSTED_BUCKET:
        logger.critical("MINIO_TRUSTED_BUCKET not configured for URL construction.")
        raise ValueError("MINIO_TRUSTED_BUCKET is not configured.")

    full_s3_url = f"s3a://{MINIO_TRUSTED_BUCKET}/{processed_key_relative}"
    return full_s3_url, processed_key_relative
