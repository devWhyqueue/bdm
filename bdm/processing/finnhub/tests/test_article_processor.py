import unittest
from datetime import datetime, timezone, timedelta

from bdm.processing.finnhub.article_processor import (
    validate_and_clean_article,
    deduplicate_articles,
    KNOWN_SOURCES_WHITELIST,
    is_valid_uri,
    clean_string
)

# Helper to create a base valid raw article
def create_base_raw_article(override: dict = None) -> dict:
    article = {
        "id": 123,
        "category": "business",
        "datetime": int((datetime.now(timezone.utc) - timedelta(minutes=30)).timestamp()),
        "headline": "Valid Headline for Test",
        "source": KNOWN_SOURCES_WHITELIST[0] if KNOWN_SOURCES_WHITELIST else "Test Source", # Ensure a valid source
        "summary": "This is a valid summary.",
        "url": "https://example.com/valid_article",
        "image": "https://example.com/valid_image.png"
    }
    if KNOWN_SOURCES_WHITELIST and "Test Source" not in KNOWN_SOURCES_WHITELIST: # Add if not present for testing
        KNOWN_SOURCES_WHITELIST.append("Test Source")

    if override:
        article.update(override)
    return article

class TestArticleProcessor(unittest.TestCase):

    def setUp(self):
        self.metadata_category = "business"
        self.scraped_at_dt = datetime.now(timezone.utc)
        # Ensure 'Test Source' is in whitelist for tests if base_raw_article uses it
        if "Test Source" not in KNOWN_SOURCES_WHITELIST:
             KNOWN_SOURCES_WHITELIST.append("Test Source")


    def test_is_valid_uri(self):
        self.assertTrue(is_valid_uri("http://example.com"))
        self.assertTrue(is_valid_uri("https://example.com/path?query=value"))
        self.assertFalse(is_valid_uri("example.com"))
        self.assertFalse(is_valid_uri(None))
        self.assertFalse(is_valid_uri(""))
        self.assertFalse(is_valid_uri("htp://example.com")) # Scheme typo

    def test_clean_string(self):
        self.assertEqual(clean_string("  text  "), "text")
        self.assertEqual(clean_string(None), None)
        self.assertEqual(clean_string(""), "")
        self.assertEqual(clean_string("no_spaces"), "no_spaces")

    def test_validate_valid_article(self):
        raw_article = create_base_raw_article()
        cleaned = validate_and_clean_article(raw_article, self.metadata_category, self.scraped_at_dt)
        self.assertIsNotNone(cleaned)
        self.assertEqual(cleaned["article_id"], raw_article["id"])
        self.assertEqual(cleaned["category"], raw_article["category"])
        self.assertEqual(cleaned["headline"], raw_article["headline"])
        self.assertEqual(cleaned["source"], raw_article["source"])
        self.assertEqual(cleaned["url"], raw_article["url"])
        self.assertEqual(cleaned["image_url"], raw_article["image"])
        self.assertEqual(cleaned["summary"], raw_article["summary"])
        self.assertIn("datetime_utc", cleaned)

    def test_validate_invalid_id(self):
        raw_article = create_base_raw_article({"id": -1})
        self.assertIsNone(validate_and_clean_article(raw_article, self.metadata_category, self.scraped_at_dt))
        raw_article = create_base_raw_article({"id": "not_an_int"})
        self.assertIsNone(validate_and_clean_article(raw_article, self.metadata_category, self.scraped_at_dt))
        raw_article = create_base_raw_article()
        del raw_article["id"]
        self.assertIsNone(validate_and_clean_article(raw_article, self.metadata_category, self.scraped_at_dt))


    def test_validate_category_mismatch(self):
        raw_article = create_base_raw_article({"category": "sports"})
        self.assertIsNone(validate_and_clean_article(raw_article, self.metadata_category, self.scraped_at_dt))

    def test_validate_invalid_datetime(self):
        # Invalid type
        raw_article = create_base_raw_article({"datetime": "not_a_timestamp"})
        self.assertIsNone(validate_and_clean_article(raw_article, self.metadata_category, self.scraped_at_dt))
        # Negative timestamp
        raw_article = create_base_raw_article({"datetime": -100})
        self.assertIsNone(validate_and_clean_article(raw_article, self.metadata_category, self.scraped_at_dt))
        # Article datetime significantly after scraped_at
        future_time = (self.scraped_at_dt + timedelta(hours=2)).timestamp()
        raw_article = create_base_raw_article({"datetime": int(future_time)})
        self.assertIsNone(validate_and_clean_article(raw_article, self.metadata_category, self.scraped_at_dt))

    def test_validate_invalid_headline(self):
        raw_article = create_base_raw_article({"headline": "    "}) # Empty after strip
        self.assertIsNone(validate_and_clean_article(raw_article, self.metadata_category, self.scraped_at_dt))
        raw_article = create_base_raw_article({"headline": "short"}) # Too short
        self.assertIsNone(validate_and_clean_article(raw_article, self.metadata_category, self.scraped_at_dt))
        raw_article = create_base_raw_article()
        del raw_article["headline"]
        self.assertIsNone(validate_and_clean_article(raw_article, self.metadata_category, self.scraped_at_dt))


    def test_validate_invalid_source(self):
        raw_article = create_base_raw_article({"source": "Unknown Source Inc."})
        self.assertIsNone(validate_and_clean_article(raw_article, self.metadata_category, self.scraped_at_dt))

    def test_validate_invalid_url(self):
        raw_article = create_base_raw_article({"url": "not_a_valid_url"})
        self.assertIsNone(validate_and_clean_article(raw_article, self.metadata_category, self.scraped_at_dt))
        raw_article = create_base_raw_article({"url": None})
        self.assertIsNone(validate_and_clean_article(raw_article, self.metadata_category, self.scraped_at_dt))

    def test_validate_invalid_image_url_is_nulled(self):
        raw_article = create_base_raw_article({"image": "not_a_valid_image_url"})
        cleaned = validate_and_clean_article(raw_article, self.metadata_category, self.scraped_at_dt)
        self.assertIsNotNone(cleaned)
        self.assertIsNone(cleaned["image_url"])

    def test_validate_empty_image_url_is_nulled(self):
        raw_article = create_base_raw_article({"image": ""})
        cleaned = validate_and_clean_article(raw_article, self.metadata_category, self.scraped_at_dt)
        self.assertIsNotNone(cleaned)
        self.assertIsNone(cleaned["image_url"])

    def test_validate_missing_optional_fields(self):
        raw_article = create_base_raw_article()
        del raw_article["summary"]
        del raw_article["image"]
        cleaned = validate_and_clean_article(raw_article, self.metadata_category, self.scraped_at_dt)
        self.assertIsNotNone(cleaned)
        self.assertEqual(cleaned["summary"], "") # Should be empty string
        self.assertIsNone(cleaned["image_url"]) # Should be None

    # Tests for deduplicate_articles
    def test_deduplicate_unique_articles(self):
        articles = [
            {"article_id": 1, "headline": "Article 1", "datetime_utc": datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)},
            {"article_id": 2, "headline": "Article 2", "datetime_utc": datetime(2023, 1, 1, 13, 0, 0, tzinfo=timezone.utc)},
        ]
        deduplicated = deduplicate_articles(articles)
        self.assertEqual(len(deduplicated), 2)

    def test_deduplicate_with_duplicates(self):
        articles = [
            {"article_id": 1, "headline": "Article 1 Old", "datetime_utc": datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)},
            {"article_id": 1, "headline": "Article 1 New", "datetime_utc": datetime(2023, 1, 1, 13, 0, 0, tzinfo=timezone.utc)}, # Newer
            {"article_id": 2, "headline": "Article 2", "datetime_utc": datetime(2023, 1, 1, 14, 0, 0, tzinfo=timezone.utc)},
        ]
        deduplicated = deduplicate_articles(articles)
        self.assertEqual(len(deduplicated), 2)
        # Check that the newer version of article_id 1 is kept
        article1 = next(a for a in deduplicated if a["article_id"] == 1)
        self.assertEqual(article1["headline"], "Article 1 New")

    def test_deduplicate_empty_list(self):
        articles = []
        deduplicated = deduplicate_articles(articles)
        self.assertEqual(len(deduplicated), 0)

    def test_deduplicate_all_duplicates_of_one_id(self):
        articles = [
            {"article_id": 1, "headline": "Article 1 Oldest", "datetime_utc": datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)},
            {"article_id": 1, "headline": "Article 1 Middle", "datetime_utc": datetime(2023, 1, 1, 13, 0, 0, tzinfo=timezone.utc)},
            {"article_id": 1, "headline": "Article 1 Newest", "datetime_utc": datetime(2023, 1, 1, 14, 0, 0, tzinfo=timezone.utc)}, # Newest
        ]
        deduplicated = deduplicate_articles(articles)
        self.assertEqual(len(deduplicated), 1)
        self.assertEqual(deduplicated[0]["headline"], "Article 1 Newest")

    def test_deduplicate_mixed_with_no_datetime(self):
        # This case should ideally not happen if validation passes, but good to test robustness
        articles = [
            {"article_id": 1, "headline": "Article 1 With Date", "datetime_utc": datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)},
            {"article_id": 1, "headline": "Article 1 No Date"}, # Missing datetime_utc
            {"article_id": 2, "headline": "Article 2"},
        ]
        # The behavior might be to keep the one with datetime_utc if present, or the last one encountered.
        # Current implementation: if current_article_dt is None, it won't be > existing.
        # If existing has no date, and current has a date, it will be chosen.
        # If both have no date, the first one encountered for that ID is kept.
        deduplicated = deduplicate_articles(articles)
        self.assertEqual(len(deduplicated), 2)
        article1 = next(a for a in deduplicated if a["article_id"] == 1)
        self.assertEqual(article1["headline"], "Article 1 With Date")


if __name__ == "__main__":
    unittest.main()
