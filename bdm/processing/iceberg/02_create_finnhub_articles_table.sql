DROP TABLE IF EXISTS catalog.finnhub_articles;
CREATE TABLE IF NOT EXISTS catalog.finnhub_articles
(
    id               BIGINT    NOT NULL,
    category         STRING    NOT NULL,
    published_at_utc TIMESTAMP NOT NULL,
    headline         STRING    NOT NULL,
    source           STRING    NOT NULL,
    summary          STRING,
    url              STRING    NOT NULL,
    image_url        STRING,
    scraped_at       TIMESTAMP NOT NULL,
    source_file      STRING    NOT NULL,
    checksum_sha256  STRING    NOT NULL
)
    USING iceberg PARTITIONED BY (days(scraped_at));
