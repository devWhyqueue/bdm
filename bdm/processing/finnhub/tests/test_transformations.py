import unittest
import json
import hashlib
from datetime import datetime, timezone, timedelta

from pyspark.sql.types import Row # For constructing mock Row objects

from bdm.processing.finnhub.transformations import (
    parse_datetime_string,
    validate_raw_json_file_udf,
    generate_checksum,
    parse_metadata_scraped_at_udf,
    validate_clean_and_deduplicate_articles_udf,
    apply_checksum_and_final_fields_udf,
)
from bdm.processing.finnhub.schemas import (
    FINNHUB_FILE_SCHEMA, # For reference, not direct validation in this test for UDF
    CLEANED_ARTICLE_STRUCT_SCHEMA,
    FINNHUB_ARTICLE_SPARK_SCHEMA
)
# Assuming KNOWN_SOURCES_WHITELIST is accessible or can be mocked if needed by underlying functions
from bdm.processing.finnhub.article_processor import KNOWN_SOURCES_WHITELIST


class TestTransformations(unittest.TestCase):

    def setUp(self):
        self.sample_scraped_at_dt = datetime.now(timezone.utc)
        self.sample_source_file = "test_source_file.json"
        # Ensure 'Test Source' is in whitelist for tests if underlying article_processor uses it
        if "Test Source" not in KNOWN_SOURCES_WHITELIST:
             KNOWN_SOURCES_WHITELIST.append("Test Source")


    def test_parse_datetime_string(self):
        self.assertIsInstance(parse_datetime_string("2023-01-01T12:00:00Z"), datetime)
        self.assertIsInstance(parse_datetime_string("2023-01-01T12:00:00+00:00"), datetime)
        self.assertIsInstance(parse_datetime_string("2023-01-01T12:00:00"), datetime) # Assumes UTC
        self.assertEqual(parse_datetime_string("2023-01-01T12:00:00Z").tzinfo, timezone.utc)
        self.assertIsNone(parse_datetime_string("invalid-date-string"))
        self.assertIsNone(parse_datetime_string(None))
        self.assertIsNone(parse_datetime_string(""))

    def test_validate_raw_json_file_udf(self):
        # Valid JSON structure according to FINNHUB_FILE_SCHEMA (simplified for this test)
        valid_data = {
            "metadata": {"category": "crypto", "article_count": 1, "scraped_at": "2023-01-01T00:00:00Z"},
            "news": [{
                "id": 1, "category": "crypto", "datetime": 1672531200, "headline": "H",
                "source": KNOWN_SOURCES_WHITELIST[0] if KNOWN_SOURCES_WHITELIST else "Test Source",
                "url": "http://example.com"
            }]
        }
        valid_json_string = json.dumps(valid_data)
        self.assertEqual(validate_raw_json_file_udf(valid_json_string), valid_json_string)

        # Invalid JSON (syntax error)
        invalid_json_syntax = '{"metadata": "missing_brace'
        self.assertIsNone(validate_raw_json_file_udf(invalid_json_syntax))

        # Valid JSON but does not match schema (e.g., missing required field 'news')
        invalid_schema_data = {"metadata": {"category": "crypto", "article_count": 0, "scraped_at": "2023-01-01T00:00:00Z"}}
        invalid_schema_json_string = json.dumps(invalid_schema_data)
        self.assertIsNone(validate_raw_json_file_udf(invalid_schema_json_string))

        # Edge case: empty string
        self.assertIsNone(validate_raw_json_file_udf(""))


    def test_generate_checksum(self):
        article_data = {
            "article_id": 123,
            "category": "news",
            "datetime_utc": datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            "headline": "Test Headline",
            "source": "Test Source",
            "summary": "Test summary.",
            "url": "http://example.com/news1",
            "image_url": "http://example.com/image1.png"
        }
        scraped_at_iso = self.sample_scraped_at_dt.isoformat()

        checksum1 = generate_checksum(article_data, scraped_at_iso, self.sample_source_file)
        checksum2 = generate_checksum(article_data, scraped_at_iso, self.sample_source_file)
        self.assertEqual(checksum1, checksum2)
        self.assertIsInstance(checksum1, str)
        self.assertEqual(len(checksum1), 64) # SHA256

        changed_article_data = article_data.copy()
        changed_article_data["headline"] = "Different Headline"
        checksum3 = generate_checksum(changed_article_data, scraped_at_iso, self.sample_source_file)
        self.assertNotEqual(checksum1, checksum3)

    def test_parse_metadata_scraped_at_udf(self):
        # Construct a mock Row object similar to how Spark UDF would receive it
        valid_metadata_row = Row(category="crypto", article_count=1, scraped_at="2023-01-01T12:30:00Z")
        parsed_dt = parse_metadata_scraped_at_udf(valid_metadata_row)
        self.assertIsInstance(parsed_dt, datetime)
        self.assertEqual(parsed_dt, datetime(2023, 1, 1, 12, 30, 0, tzinfo=timezone.utc))

        invalid_metadata_row = Row(category="crypto", article_count=1, scraped_at="invalid-date")
        self.assertIsNone(parse_metadata_scraped_at_udf(invalid_metadata_row))

        empty_metadata_row = Row(category="crypto", article_count=1, scraped_at=None)
        self.assertIsNone(parse_metadata_scraped_at_udf(empty_metadata_row))

        null_row = None
        self.assertIsNone(parse_metadata_scraped_at_udf(null_row))


    def test_validate_clean_and_deduplicate_articles_udf(self):
        metadata_category = "general"
        # Ensure the source is in the whitelist for the underlying validation
        valid_source = KNOWN_SOURCES_WHITELIST[0] if KNOWN_SOURCES_WHITELIST else "Test Source"

        raw_article_valid_dict = {
            "id": 1, "category": "general", "datetime": int(self.sample_scraped_at_dt.timestamp()) - 1000,
            "headline": "Valid news", "source": valid_source, "url": "http://example.com/news/1",
            "summary": "A summary", "image": "http://example.com/image.png"
        }
        raw_article_invalid_dict = { # Invalid headline
            "id": 2, "category": "general", "datetime": int(self.sample_scraped_at_dt.timestamp()) - 1000,
            "headline": "", "source": valid_source, "url": "http://example.com/news/2"
        }
        raw_article_duplicate_dict = { # Duplicate of id 1, but newer
            "id": 1, "category": "general", "datetime": int(self.sample_scraped_at_dt.timestamp()) - 500,
            "headline": "Valid news updated", "source": valid_source, "url": "http://example.com/news/1/updated",
        }

        # Spark UDFs receive a list of Row objects if ArrayType(StructType) is input
        raw_articles_list_of_rows = [
            Row(**raw_article_valid_dict),
            Row(**raw_article_invalid_dict), # This one should be filtered out by validate_and_clean_article
            Row(**raw_article_duplicate_dict) # This one should replace the first valid one due to newer datetime
        ]

        cleaned_list_of_dicts = validate_clean_and_deduplicate_articles_udf(
            raw_articles_list_of_rows, metadata_category, self.sample_scraped_at_dt
        )

        self.assertIsNotNone(cleaned_list_of_dicts)
        self.assertEqual(len(cleaned_list_of_dicts), 1) # Only one article should remain

        final_article = cleaned_list_of_dicts[0]
        self.assertEqual(final_article["article_id"], 1)
        self.assertEqual(final_article["headline"], "Valid news updated")
        self.assertIn("datetime_utc", final_article)
        self.assertIsInstance(final_article["datetime_utc"], datetime)

        # Test with empty input
        self.assertIsNone(validate_clean_and_deduplicate_articles_udf([], metadata_category, self.sample_scraped_at_dt))
        self.assertIsNone(validate_clean_and_deduplicate_articles_udf(None, metadata_category, self.sample_scraped_at_dt))

    def test_apply_checksum_and_final_fields_udf(self):
        cleaned_article_data = {
            "article_id": 789,
            "category": "tech",
            "datetime_utc": self.sample_scraped_at_dt - timedelta(hours=1),
            "headline": "Tech Giants Announce Merger",
            "source": KNOWN_SOURCES_WHITELIST[0] if KNOWN_SOURCES_WHITELIST else "Test Source",
            "summary": "A detailed summary of the merger.",
            "url": "https://example.com/tech_merger",
            "image_url": "https://example.com/merger_image.jpg"
        }
        # UDF expects a Row object for the cleaned article struct
        cleaned_article_row = Row(**cleaned_article_data)

        scraped_at_for_udf = self.sample_scraped_at_dt
        source_file_for_udf = "s3://bucket/file.json"

        iceberg_record_dict = apply_checksum_and_final_fields_udf(
            cleaned_article_row, scraped_at_for_udf, source_file_for_udf
        )

        self.assertIsNotNone(iceberg_record_dict)
        self.assertIsInstance(iceberg_record_dict, dict) # UDF is defined to return FINNHUB_ARTICLE_SPARK_SCHEMA which is a StructType, results in dict-like

        # Check if all fields from FINNHUB_ARTICLE_SPARK_SCHEMA are present
        for field in FINNHUB_ARTICLE_SPARK_SCHEMA.fields:
            self.assertIn(field.name, iceberg_record_dict)

        self.assertEqual(iceberg_record_dict["article_id"], cleaned_article_data["article_id"])
        self.assertEqual(iceberg_record_dict["headline"], cleaned_article_data["headline"])
        self.assertEqual(iceberg_record_dict["scraped_at"], scraped_at_for_udf)
        self.assertEqual(iceberg_record_dict["source_file"], source_file_for_udf)
        self.assertIsNotNone(iceberg_record_dict["checksum_sha256"])
        self.assertEqual(len(iceberg_record_dict["checksum_sha256"]), 64)

        # Test with None input for cleaned_article_struct
        self.assertIsNone(apply_checksum_and_final_fields_udf(None, scraped_at_for_udf, source_file_for_udf))


if __name__ == "__main__":
    unittest.main()
