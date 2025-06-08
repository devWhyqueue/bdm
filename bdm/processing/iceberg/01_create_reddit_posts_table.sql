-- Drop the table if it exists to ensure schema update (for dev/testing purposes)
-- In production, you might use ALTER TABLE for some schema changes if supported and non-breaking.
-- However, for complex type additions like ARRAY<STRUCT>, recreating might be simpler
-- if the table can be repopulated. Given this is an initial setup, DROP/CREATE is fine.
DROP TABLE IF EXISTS catalog.reddit_posts;

CREATE TABLE IF NOT EXISTS catalog.reddit_posts (
  id                     STRING   NOT NULL,
  title                  STRING   NOT NULL,
  author                 STRING   NOT NULL,
  created_utc            TIMESTAMP NOT NULL,
  score                  INT      NOT NULL,
  url                    STRING   NOT NULL,
  selftext               STRING,
  num_comments           INT      NOT NULL,
  permalink              STRING   NOT NULL,
  upvote_ratio           DOUBLE   NOT NULL,
  is_original_content    BOOLEAN  NOT NULL,
  is_self                BOOLEAN  NOT NULL,
  subreddit              STRING   NOT NULL,
  scraped_at             TIMESTAMP NOT NULL,
  source_file            STRING   NOT NULL,
  checksum_sha256        STRING   NOT NULL,
  media_items            ARRAY<STRUCT<
                           media_type: STRING,
                           s3_url: STRING,         -- Original S3 URL (relative path from LZ JSON)
                           source_url: STRING,     -- Original source URL from Reddit
                           filename: STRING,
                           content_type: STRING,   -- Original content_type from LZ JSON
                           processed_s3_url: STRING, -- Full S3 URL to the processed media in trusted/processed zone
                           processed_format: STRING, -- e.g., 'jpeg', 'mp4'
                           processed_size_bytes: BIGINT,
                           validation_error: STRING, -- Error message if validation (e.g. MIME type) failed
                           conversion_error: STRING, -- Error message if conversion (e.g. ffmpeg) failed
                           size_error: STRING      -- Error message if size check failed (e.g. > 50MB)
                         >>
)
USING iceberg
PARTITIONED BY (days(scraped_at));

-- Optional: Add a comment to the table or columns for documentation
-- COMMENT ON TABLE catalog.reddit_posts IS 'Stores processed Reddit post data, including media item metadata.';
-- COMMENT ON COLUMN catalog.reddit_posts.media_items IS 'Array of processed media items associated with the post. Media files stored in a separate S3 location.';
