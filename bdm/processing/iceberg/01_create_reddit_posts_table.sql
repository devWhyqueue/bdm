-- Create 'reddit_posts' table, if it does not already exist:
CREATE TABLE IF NOT EXISTS catalog.reddit_posts
(
    post_id             STRING    NOT NULL,
    title               STRING    NOT NULL,
    author              STRING    NOT NULL,
    created_utc         TIMESTAMP NOT NULL,
    score               INT       NOT NULL,
    url                 STRING    NOT NULL,
    selftext            STRING,
    num_comments        INT       NOT NULL,
    permalink           STRING    NOT NULL,
    upvote_ratio        DOUBLE    NOT NULL,
    is_original_content BOOLEAN   NOT NULL,
    is_self             BOOLEAN   NOT NULL,
    subreddit           STRING    NOT NULL,
    scraped_at          TIMESTAMP NOT NULL,
    source_file         STRING    NOT NULL,
    checksum_sha256     STRING    NOT NULL
)
    USING iceberg PARTITIONED BY (days(scraped_at));
