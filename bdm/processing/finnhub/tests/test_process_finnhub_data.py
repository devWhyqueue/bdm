import unittest
from unittest.mock import patch, MagicMock, call
import json
from datetime import datetime, timezone, timedelta

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType

from bdm.utils import create_spark_session # Shared Spark session utility
from bdm.processing.finnhub.process_finnhub_data import process_finnhub_data_spark
from bdm.processing.finnhub.article_processor import KNOWN_SOURCES_WHITELIST # For test data

# Sample valid Finnhub JSON data string
def create_sample_json_string(
        category="crypto",
        article_id=1,
        headline="Test Headline",
        scraped_at_str="2023-10-26T10:00:00Z",
        article_datetime_offset_minutes=-30, # article time relative to scraped_at
        source=None,
        url="http://example.com/article1",
        article_count=1
    ):
    if source is None:
        source = KNOWN_SOURCES_WHITELIST[0] if KNOWN_SOURCES_WHITELIST else "TestValidSource"
        if source == "TestValidSource" and "TestValidSource" not in KNOWN_SOURCES_WHITELIST:
            KNOWN_SOURCES_WHITELIST.append("TestValidSource")


    scraped_at_dt = datetime.fromisoformat(scraped_at_str.replace("Z", "+00:00"))
    article_dt = scraped_at_dt + timedelta(minutes=article_datetime_offset_minutes)

    data = {
        "metadata": {"category": category, "article_count": article_count, "scraped_at": scraped_at_str},
        "news": [{
            "id": article_id, "category": category, "datetime": int(article_dt.timestamp()),
            "headline": headline, "source": source, "summary": "A summary", "url": url,
            "image": "http://example.com/image.png"
        }]
    }
    if article_count > 1: # Add more articles if article_count suggests so
        for i in range(2, article_count + 1):
            data["news"].append({
                 "id": article_id + i -1, "category": category, "datetime": int(article_dt.timestamp()) + i*10,
                "headline": f"{headline} #{i}", "source": source, "summary": f"A summary {i}",
                "url": f"{url}/{i}", "image": f"http://example.com/image{i}.png"
            })
    return json.dumps(data)

class TestProcessFinnhubDataIntegration(unittest.TestCase):
    spark: SparkSession = None

    @classmethod
    def setUpClass(cls):
        # Ensure 'TestValidSource' is in whitelist for data generation if not already
        if "TestValidSource" not in KNOWN_SOURCES_WHITELIST:
            KNOWN_SOURCES_WHITELIST.append("TestValidSource")
        cls.spark = create_spark_session(app_name="FinnhubIntegrationTest")

    @classmethod
    def tearDownClass(cls):
        if cls.spark:
            cls.spark.stop()

    def create_df_from_strings(self, data_tuples):
        # data_tuples is a list of (file_name, json_string_content)
        rows = [Row(value=content, source_file_name=name) for name, content in data_tuples]
        schema = StructType([
            StructField("value", StringType(), True),
            StructField("source_file_name", StringType(), True)
        ])
        return self.spark.createDataFrame(rows, schema)

    @patch("bdm.processing.finnhub.process_finnhub_data.write_df_to_iceberg")
    @patch("bdm.processing.finnhub.process_finnhub_data.verify_iceberg_write")
    @patch("bdm.processing.finnhub.process_finnhub_data.read_raw_json_files_with_source")
    def test_process_single_valid_file(self, mock_read_raw, mock_verify, mock_write):
        valid_json_content = create_sample_json_string(article_id=100, headline="Valid Integration Test")
        mock_df = self.create_df_from_strings([("valid_file_1.json", valid_json_content)])
        mock_read_raw.return_value = mock_df
        mock_write.return_value = True
        mock_verify.return_value = True

        result = process_finnhub_data_spark(self.spark, "s3a://test-bucket/valid_path/*.json")

        self.assertTrue(result)
        mock_read_raw.assert_called_once_with(self.spark, "s3a://test-bucket/valid_path/*.json")
        mock_write.assert_called_once()

        # Check the DataFrame passed to write_df_to_iceberg
        written_df = mock_write.call_args[0][1] # Positional arg df is the second one
        self.assertEqual(written_df.count(), 1)
        written_data = written_df.collect()[0]
        self.assertEqual(written_data["article_id"], 100)
        self.assertEqual(written_data["headline"], "Valid Integration Test")
        self.assertIsNotNone(written_data["checksum_sha256"])

        mock_verify.assert_called_once()


    @patch("bdm.processing.finnhub.process_finnhub_data.write_df_to_iceberg")
    @patch("bdm.processing.finnhub.process_finnhub_data.verify_iceberg_write")
    @patch("bdm.processing.finnhub.process_finnhub_data.read_raw_json_files_with_source")
    def test_process_file_schema_validation_failure(self, mock_read_raw, mock_verify, mock_write):
        invalid_json_content = '{"metadata": "this is not valid json,,,,,"}' # Syntax error
        mock_df = self.create_df_from_strings([("invalid_file_schema.json", invalid_json_content)])
        mock_read_raw.return_value = mock_df

        result = process_finnhub_data_spark(self.spark, "s3a://test-bucket/invalid_schema_path/*.json")

        self.assertTrue(result) # Should still be true as it's a handled error for one file
        mock_write.assert_not_called() # No valid data to write
        mock_verify.assert_not_called()


    @patch("bdm.processing.finnhub.process_finnhub_data.write_df_to_iceberg")
    @patch("bdm.processing.finnhub.process_finnhub_data.verify_iceberg_write")
    @patch("bdm.processing.finnhub.process_finnhub_data.read_raw_json_files_with_source")
    def test_process_file_article_validation_failure(self, mock_read_raw, mock_verify, mock_write):
        # Valid JSON schema, but article content is invalid (e.g., bad headline)
        invalid_article_content = create_sample_json_string(headline="  ", article_count=1) # Invalid headline
        mock_df = self.create_df_from_strings([("invalid_article_file.json", invalid_article_content)])
        mock_read_raw.return_value = mock_df

        result = process_finnhub_data_spark(self.spark, "s3a://test-bucket/invalid_article_path/*.json")

        self.assertTrue(result) # Should be true, indicates successful run even if no data written
        mock_write.assert_not_called()
        mock_verify.assert_not_called()

    @patch("bdm.processing.finnhub.process_finnhub_data.write_df_to_iceberg")
    @patch("bdm.processing.finnhub.process_finnhub_data.verify_iceberg_write")
    @patch("bdm.processing.finnhub.process_finnhub_data.read_raw_json_files_with_source")
    def test_process_multiple_files_mixed_validity(self, mock_read_raw, mock_verify, mock_write):
        valid_json_1 = create_sample_json_string(article_id=201, headline="Valid File 1")
        # This file's articles will be dropped due to count consistency check (N_cleaned < metadata_article_count / 2)
        # metadata.article_count = 5, but only 1 valid article in "news"
        low_quality_json = create_sample_json_string(category="general", article_id=301, headline="Low Quality File", article_count=5)
        # Modify low_quality_json to only have one article in news, while metadata says 5
        temp_data = json.loads(low_quality_json)
        temp_data["news"] = [temp_data["news"][0]] # Keep only one article
        low_quality_json_modified = json.dumps(temp_data)

        invalid_schema_json = '{"corrupted": true}'

        mock_df = self.create_df_from_strings([
            ("valid_file_1.json", valid_json_1),
            ("low_quality_file.json", low_quality_json_modified),
            ("invalid_schema_file.json", invalid_schema_json)
        ])
        mock_read_raw.return_value = mock_df
        mock_write.return_value = True
        mock_verify.return_value = True

        result = process_finnhub_data_spark(self.spark, "s3a://test-bucket/mixed_path/*.json")

        self.assertTrue(result) # Overall process should succeed
        mock_write.assert_called_once() # Only valid_file_1 should be written
        written_df = mock_write.call_args[0][1]
        self.assertEqual(written_df.count(), 1)
        self.assertEqual(written_df.collect()[0]["article_id"], 201)

        # verify_iceberg_write should also only be called for the successfully written file
        mock_verify.assert_called_once()
        self.assertEqual(mock_verify.call_args[1]["source_file_name"], "valid_file_1.json")


    @patch("bdm.processing.finnhub.process_finnhub_data.write_df_to_iceberg")
    @patch("bdm.processing.finnhub.process_finnhub_data.verify_iceberg_write")
    @patch("bdm.processing.finnhub.process_finnhub_data.read_raw_json_files_with_source")
    def test_write_failure_handled(self, mock_read_raw, mock_verify, mock_write):
        valid_json_content = create_sample_json_string(article_id=400)
        mock_df = self.create_df_from_strings([("write_fail_file.json", valid_json_content)])
        mock_read_raw.return_value = mock_df
        mock_write.return_value = False # Simulate write failure
        # mock_verify should not be called if write fails

        result = process_finnhub_data_spark(self.spark, "s3a://test-bucket/write_fail_path/*.json")

        self.assertFalse(result) # Overall success should be False due to write failure
        mock_write.assert_called_once()
        mock_verify.assert_not_called()

    @patch("bdm.processing.finnhub.process_finnhub_data.write_df_to_iceberg")
    @patch("bdm.processing.finnhub.process_finnhub_data.verify_iceberg_write")
    @patch("bdm.processing.finnhub.process_finnhub_data.read_raw_json_files_with_source")
    def test_verify_failure_handled(self, mock_read_raw, mock_verify, mock_write):
        valid_json_content = create_sample_json_string(article_id=500)
        mock_df = self.create_df_from_strings([("verify_fail_file.json", valid_json_content)])
        mock_read_raw.return_value = mock_df
        mock_write.return_value = True # Write succeeds
        mock_verify.return_value = False # Simulate verify failure

        result = process_finnhub_data_spark(self.spark, "s3a://test-bucket/verify_fail_path/*.json")

        self.assertFalse(result) # Overall success should be False due to verify failure
        mock_write.assert_called_once()
        mock_verify.assert_called_once()


if __name__ == "__main__":
    unittest.main()
