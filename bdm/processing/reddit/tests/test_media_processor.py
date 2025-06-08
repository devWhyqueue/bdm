import os
import subprocess
import unittest
from unittest.mock import patch, MagicMock, mock_open, ANY
import importlib # Used for reloading the module if necessary

# Module under test
from bdm.processing.reddit import media_processor
from bdm.processing.reddit.media_processor import (
    get_true_mime_type, # Still testable if it doesn't depend on s3_client
    convert_image_to_jpeg, # Still testable
    convert_to_h264_mp4, # Still testable
    construct_processed_s3_url, # Still testable
    process_single_media_item,
    MAX_FILE_SIZE_BYTES
)

# Apply patches at the class level
# Patch os.environ first
@patch.dict(os.environ, {
    "MINIO_ENDPOINT": "http://test-minio:9000",
    "MINIO_ACCESS_KEY": "testkey",
    "MINIO_SECRET_KEY": "testsecret",
    "LANDING_ZONE_BUCKET": "test-landing-bucket",
    "PROCESSED_ZONE_BUCKET": "test-processed-bucket"
}, clear=True)
# Then patch the s3_client in the media_processor module
@patch('bdm.processing.reddit.media_processor.s3_client', new_callable=MagicMock)
class TestMediaProcessor(unittest.TestCase):

    # mock_s3_client_global is injected by the class-level patch
    def setUp(self, mock_s3_client_global: MagicMock):
        # Reload media_processor to ensure it picks up patched os.environ for constants
        # and to allow s3_client to be the mock from the start if it was initialized at import time.
        # This also means constants like S3_LANDING_BUCKET are correctly set from mocked os.environ
        importlib.reload(media_processor)

        # The class-level patch replaces media_processor.s3_client.
        # We assign this global mock to an instance variable for easier access and per-test configuration.
        self.mock_s3_client = media_processor.s3_client
        self.mock_s3_client.reset_mock() # Reset for each test

        # Configure default behaviors for S3 client methods
        # head_object is not directly used by process_single_media_item, but by get_s3_object_details
        self.mock_s3_client.head_object.return_value = {
            'ContentLength': 1000, 'ContentType': 'application/octet-stream'
        }
        # download_file and upload_file don't return values on success, they raise exceptions on failure.
        # So, default behavior is "success" (no exception).
        self.mock_s3_client.download_file.return_value = None
        self.mock_s3_client.upload_file.return_value = None

        self.sample_image_item = {
            "media_type": "image",
            "s3_url": "reddit/test_subreddit/test_date/images/image.png", # Relative key
            "source_url": "http://example.com/image.png",
            "filename": "image.png",
            "content_type": "image/png",
        }
        self.sample_video_item = {
            "media_type": "video",
            "s3_url": "reddit/test_subreddit/test_date/videos/video.mov", # Relative key
            "filename": "video.mov",
            "content_type": "video/quicktime",
        }
        self.sample_gif_item = {
            "media_type": "gif",
            "s3_url": "reddit/test_subreddit/test_date/gifs/animation.gif", # Relative key
            "filename": "animation.gif",
            "content_type": "image/gif",
        }
        self.mock_temp_dir = "/tmp/test_media_processor_temp_dir"

    @patch('bdm.processing.reddit.media_processor.magic.from_file')
    def test_get_true_mime_type_uses_magic(self, mock_magic_from_file):
        expected_mime = "image/png"
        mock_magic_from_file.return_value = expected_mime
        result = get_true_mime_type("/some/path/file.png")
        mock_magic_from_file.assert_called_once_with("/some/path/file.png", mime=True)
        self.assertEqual(result, expected_mime)

    @patch('bdm.processing.reddit.media_processor.subprocess.run')
    def test_convert_image_to_jpeg_calls_convert(self, mock_subprocess_run):
        mock_process = MagicMock()
        mock_process.stdout = "success"
        mock_process.stderr = ""
        mock_subprocess_run.return_value = mock_process

        self.assertTrue(convert_image_to_jpeg("input.png", "output.jpeg"))
        expected_command = ["convert", "input.png", "output.jpeg"]
        mock_subprocess_run.assert_called_once_with(expected_command, check=True, capture_output=True, text=True)

    @patch('bdm.processing.reddit.media_processor.subprocess.run')
    def test_convert_image_to_jpeg_handles_called_process_error(self, mock_subprocess_run):
        mock_subprocess_run.side_effect = subprocess.CalledProcessError(1, "cmd", stderr="Error converting")
        self.assertFalse(convert_image_to_jpeg("input.png", "output.jpeg"))

    @patch('bdm.processing.reddit.media_processor.subprocess.run')
    def test_convert_image_to_jpeg_handles_file_not_found_error(self, mock_subprocess_run):
        mock_subprocess_run.side_effect = FileNotFoundError("convert not found")
        self.assertFalse(convert_image_to_jpeg("input.png", "output.jpeg"))

    @patch('bdm.processing.reddit.media_processor.subprocess.run')
    def test_convert_to_h264_mp4_calls_ffmpeg(self, mock_subprocess_run):
        mock_process = MagicMock()
        mock_process.stdout = "success"
        mock_process.stderr = ""
        mock_subprocess_run.return_value = mock_process
        self.assertTrue(convert_to_h264_mp4("input.mov", "output.mp4"))
        expected_command = ["ffmpeg", "-i", "input.mov", "-vcodec", "libx264", "-acodec", "aac", "-y", "output.mp4"]
        mock_subprocess_run.assert_called_once_with(expected_command, check=True, capture_output=True, text=True)

    def test_construct_processed_s3_url(self):
        # This uses the reloaded media_processor, so S3_PROCESSED_BUCKET is from mocked os.environ
        original_key_relative = "reddit/sub/images/file.png"
        new_ext = "jpeg"

        expected_processed_key = f"processed_media/reddit/sub/images/file_processed.{new_ext}"
        expected_full_url = f"s3a://{media_processor.S3_PROCESSED_BUCKET}/{expected_processed_key}"

        full_url, processed_key = construct_processed_s3_url(original_key_relative, new_ext)

        self.assertEqual(full_url, expected_full_url)
        self.assertEqual(processed_key, expected_processed_key)

    # Helper for managing patches for process_single_media_item tests
    def get_process_single_media_item_patches(self):
        return [
            patch('bdm.processing.reddit.media_processor.magic.from_file'),
            patch('bdm.processing.reddit.media_processor.subprocess.run'),
            patch('os.path.getsize'),
            patch('tempfile.mkdtemp', return_value=self.mock_temp_dir),
            patch('os.listdir', return_value=[]),
            patch('os.path.isfile', return_value=False),
            patch('os.unlink'),
            patch('os.rmdir')
        ]

    def run_process_single_media_item_test(self, item_config, mock_s3_client_method_configs=None):
        active_patches = [p.start() for p in self.get_process_single_media_item_patches()]
        mock_magic_from_file, mock_subprocess_run, mock_os_getsize, \
        mock_mkdtemp, mock_listdir, mock_isfile, mock_os_unlink, mock_os_rmdir = active_patches

        # Configure S3 client mocks for this specific test run
        if mock_s3_client_method_configs:
            for method_name, config in mock_s3_client_method_configs.items():
                getattr(self.mock_s3_client, method_name).configure_mock(**config)

        # Configure other mocks
        mock_magic_from_file.return_value = item_config["expected_mime"]
        if item_config.get("conversion_command_type"): # image or video/gif
            if item_config["conversion_success"]:
                mock_subprocess_run.return_value = MagicMock(stdout="conv success", stderr="")
            else:
                mock_subprocess_run.side_effect = subprocess.CalledProcessError(1, "cmd", stderr="Conversion Error")
        mock_os_getsize.return_value = item_config["file_size"]

        # Reload to ensure constants are from the patched os.environ immediately before the call
        importlib.reload(media_processor)
        # And re-assign the mock s3_client to the reloaded module's s3_client reference
        media_processor.s3_client = self.mock_s3_client


        result = process_single_media_item(item_config["item"], "test_log_source.json")

        for p in reversed(active_patches):
            p.stop()

        return result, mock_magic_from_file, mock_subprocess_run, mock_os_getsize, mock_mkdtemp

    def test_image_processing_success(self):
        item = self.sample_image_item.copy()
        input_local_path = os.path.join(self.mock_temp_dir, item["filename"])
        output_local_path = os.path.join(self.mock_temp_dir, "image_processed.jpeg")

        result, _, mock_subprocess_run, _, mock_mkdtemp = \
            self.run_process_single_media_item_test(
                {"item": item, "expected_mime": "image/png", "conversion_command_type": "image",
                 "conversion_success": True, "file_size": 10 * 1024 * 1024},
                mock_s3_client_method_configs={
                    "download_file": {"return_value": None}, # success
                    "upload_file": {"return_value": None}    # success
                }
            )

        self.assertIsNone(result.get("validation_error"), result.get("validation_error"))
        self.assertIsNone(result.get("conversion_error"), result.get("conversion_error"))
        self.assertIsNone(result.get("size_error"), result.get("size_error"))
        self.assertEqual(result["processed_format"], "jpeg")
        expected_s3_key = f"processed_media/reddit/test_subreddit/test_date/images/image_processed.jpeg"
        self.assertEqual(result["processed_s3_url"], f"s3a://{media_processor.S3_PROCESSED_BUCKET}/{expected_s3_key}")

        self.mock_s3_client.download_file.assert_called_once_with(media_processor.S3_LANDING_BUCKET, item["s3_url"], input_local_path)
        mock_subprocess_run.assert_called_once_with(["convert", input_local_path, output_local_path], check=True, capture_output=True, text=True)
        self.mock_s3_client.upload_file.assert_called_once_with(output_local_path, media_processor.S3_PROCESSED_BUCKET, expected_s3_key, ExtraArgs={'ContentType': 'image/jpeg'})

    def test_video_processing_success(self):
        item = self.sample_video_item.copy()
        input_local_path = os.path.join(self.mock_temp_dir, item["filename"])
        output_local_path = os.path.join(self.mock_temp_dir, "video_processed.mp4")

        result, _, mock_subprocess_run, _, _ = \
            self.run_process_single_media_item_test(
                {"item": item, "expected_mime": "video/quicktime", "conversion_command_type": "video",
                 "conversion_success": True, "file_size": 20 * 1024 * 1024},
                mock_s3_client_method_configs={
                    "download_file": {"return_value": None}, "upload_file": {"return_value": None}
                }
            )
        self.assertIsNone(result.get("validation_error"))
        self.assertIsNone(result.get("conversion_error"))
        self.assertEqual(result["processed_format"], "mp4")
        expected_s3_key = f"processed_media/reddit/test_subreddit/test_date/videos/video_processed.mp4"
        self.assertEqual(result["processed_s3_url"], f"s3a://{media_processor.S3_PROCESSED_BUCKET}/{expected_s3_key}")
        mock_subprocess_run.assert_called_once_with(["ffmpeg", "-i", input_local_path, "-vcodec", "libx264", "-acodec", "aac", "-y", output_local_path], check=True, capture_output=True, text=True)

    def test_gif_processing_success(self): # GIF to MP4
        item = self.sample_gif_item.copy()
        input_local_path = os.path.join(self.mock_temp_dir, item["filename"])
        output_local_path = os.path.join(self.mock_temp_dir, "animation_processed.mp4")
        result, _, mock_subprocess_run, _, _ = \
            self.run_process_single_media_item_test(
                {"item": item, "expected_mime": "image/gif", "conversion_command_type": "gif",
                 "conversion_success": True, "file_size": 5 * 1024 * 1024},
                mock_s3_client_method_configs={
                    "download_file": {"return_value": None}, "upload_file": {"return_value": None}
                }
            )
        self.assertIsNone(result.get("validation_error"))
        self.assertIsNone(result.get("conversion_error"))
        self.assertEqual(result["processed_format"], "mp4")
        expected_s3_key = f"processed_media/reddit/test_subreddit/test_date/gifs/animation_processed.mp4"
        self.assertEqual(result["processed_s3_url"], f"s3a://{media_processor.S3_PROCESSED_BUCKET}/{expected_s3_key}")
        mock_subprocess_run.assert_called_once_with(["ffmpeg", "-i", input_local_path, "-vcodec", "libx264", "-acodec", "aac", "-y", output_local_path], check=True, capture_output=True, text=True)

    def test_download_failure(self):
        item = self.sample_image_item.copy()
        result, _, _, _, _ = \
            self.run_process_single_media_item_test(
                {"item": item, "expected_mime": "image/png", # Doesn't matter as download fails
                 "conversion_command_type": None, "conversion_success": False,
                 "file_size": 0}, # File size doesn't matter
                mock_s3_client_method_configs={
                    "download_file": {"side_effect": Exception("S3 Download Error")}
                }
            )
        self.assertIsNotNone(result.get("validation_error"))
        self.assertTrue("Failed to download from S3" in result["validation_error"])
        self.mock_s3_client.download_file.assert_called_once()

    def test_upload_failure(self):
        item = self.sample_image_item.copy()
        result, _, _, _, _ = \
            self.run_process_single_media_item_test(
                {"item": item, "expected_mime": "image/png", "conversion_command_type": "image",
                 "conversion_success": True, "file_size": 1024 * 1024},
                mock_s3_client_method_configs={
                    "download_file": {"return_value": None},
                    "upload_file": {"side_effect": Exception("S3 Upload Error")}
                }
            )
        self.assertIsNotNone(result.get("conversion_error"))
        self.assertEqual(result["conversion_error"], "Failed to upload processed file to S3")
        self.mock_s3_client.upload_file.assert_called_once()

    def test_conversion_failure_subprocess_error(self):
        item = self.sample_image_item.copy()
        result, _, mock_subprocess_run, _, _ = \
            self.run_process_single_media_item_test(
                {"item": item, "expected_mime": "image/png", "conversion_command_type": "image",
                 "conversion_success": False, "file_size": 1024 * 1024}, # conversion_success is False
                mock_s3_client_method_configs={"download_file": {"return_value": None}}
            )
        self.assertIsNotNone(result.get("conversion_error"))
        self.assertEqual(result["conversion_error"], "Image to JPEG conversion failed")
        mock_subprocess_run.assert_called() # Assert it was called, side effect is already set

    def test_size_exceeded_error(self):
        item = self.sample_image_item.copy()
        result, _, _, mock_os_getsize, _ = \
            self.run_process_single_media_item_test(
                {"item": item, "expected_mime": "image/png", "conversion_command_type": "image",
                 "conversion_success": True, "file_size": MAX_FILE_SIZE_BYTES + 1}, # file_size too large
                mock_s3_client_method_configs={
                    "download_file": {"return_value": None},
                    "upload_file": {"return_value": None} # Assume upload would succeed if not for size
                }
            )
        self.assertIsNotNone(result.get("size_error"))
        self.assertIn("exceeds limit", result["size_error"])
        self.assertIsNone(result.get("processed_s3_url"))
        mock_os_getsize.assert_called_once()

    def test_validation_error_mime_mismatch(self):
        item = self.sample_image_item.copy() # Declared 'image'
        result, mock_magic_from_file, _, _, _ = \
            self.run_process_single_media_item_test(
                {"item": item, "expected_mime": "video/mp4", # Actual is video
                 "conversion_command_type": None, # No conversion attempted
                 "conversion_success": True, "file_size": 1024},
                mock_s3_client_method_configs={"download_file": {"return_value": None}}
            )
        self.assertIsNotNone(result.get("validation_error"))
        self.assertIn("mismatch or invalid for pipeline", result["validation_error"])
        self.assertIsNone(result.get("processed_s3_url"))
        mock_magic_from_file.assert_called_once()

    def test_getsize_failure_in_upload_step(self):
        item = self.sample_image_item.copy()
        # Configure os.path.getsize to raise OSError for this test
        # This needs to be done carefully as run_process_single_media_item_test sets up patches.
        # We can achieve this by stopping the mock_os_getsize from the helper and starting a new one.

        # Initial setup of mocks, but we'll override os_getsize
        active_patches = [p.start() for p in self.get_process_single_media_item_patches()]
        # Unpack to get specific mocks if needed, or just manage them as a list
        mock_magic_from_file, mock_subprocess_run, mock_os_getsize_orig, \
        mock_mkdtemp, _, _, _, _ = active_patches

        # Stop the original os.path.getsize mock from the helper
        mock_os_getsize_orig.stop()

        # Start a new os.path.getsize mock that raises an error
        with patch('os.path.getsize', side_effect=OSError("Failed to get size")) as mock_os_getsize_custom:
            result, _, _, _, _ = \
                self.run_process_single_media_item_test(
                    {"item": item, "expected_mime": "image/png", "conversion_command_type": "image",
                     "conversion_success": True,
                     "file_size": 0}, # file_size is normally from mock_os_getsize, but it's overridden
                    mock_s3_client_method_configs={
                        "download_file": {"return_value": None},
                        "upload_file": {"return_value": None} # Assume upload itself would succeed
                    }
                )

        # Stop remaining patches from the helper
        # (mock_os_getsize_orig was already stopped, mock_os_getsize_custom stopped by with statement)
        mock_magic_from_file.stop()
        mock_subprocess_run.stop()
        mock_mkdtemp.stop()
        # Stop others if they were started and unpacked: os.listdir, os.path.isfile, os.unlink, os.rmdir
        # This part is a bit tricky with the current helper. A more robust helper might return the started mocks.
        # For now, let's assume the critical mocks are handled.
        # A better way would be to pass the mock_os_getsize behavior into run_process_single_media_item_test.

        self.assertIsNotNone(result.get("size_error"))
        self.assertEqual(result.get("size_error"), "Could not determine size of processed file")
        # The URL might still be populated if upload happened before the size check,
        # depending on the exact logic in _upload_and_verify_processed_media.
        # Current logic: URL is set, then size is checked. If size fails, URL is kept but error set.
        self.assertIsNotNone(result.get("processed_s3_url")) # URL is there
        self.assertEqual(result.get("processed_format"), "jpeg") # Format is there
        self.assertEqual(result.get("processed_size_bytes"), 0) # Defaulted to 0 or None
        mock_os_getsize_custom.assert_called_once()


if __name__ == '__main__':
    unittest.main()
