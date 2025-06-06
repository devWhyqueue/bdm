-- Describe reddit_posts table
DESCRIBE
TABLE catalog.reddit_posts;

-- Describe finnhub_articles table
DESCRIBE
TABLE catalog.finnhub_articles;

-- Insert a sample row into reddit_posts
INSERT INTO catalog.reddit_posts
VALUES ('test_post_id',
        'Test Title',
        'Test Author',
        TIMESTAMP '2023-01-01 12:00:00 UTC',
        100,
        'http://example.com/test_post',
        'This is a test selftext.',
        10,
        '/r/test/comments/test_post_id',
        0.9,
        true,
        true,
        'test_subreddit',
        TIMESTAMP '2023-01-02 10:00:00 UTC',
        'test_source_file.json',
        'test_checksum');

-- Insert a sample row into finnhub_articles
INSERT INTO catalog.finnhub_articles
VALUES (12345,
        'Test Category',
        TIMESTAMP '2023-01-01 14:00:00 UTC',
        'Test Headline',
        'Test Source',
        'This is a test summary.',
        'http://example.com/test_article',
        'http://example.com/test_image.jpg',
        TIMESTAMP '2023-01-02 11:00:00 UTC',
        'test_article_source.json',
        'test_article_checksum');

-- Select sample data from reddit_posts
SELECT *
FROM catalog.reddit_posts
WHERE post_id = 'test_post_id';

-- Select sample data from finnhub_articles
SELECT *
FROM catalog.finnhub_articles
WHERE article_id = 12345;

-- Clean up sample data from reddit_posts
DELETE
FROM catalog.reddit_posts
WHERE post_id = 'test_post_id';

-- Clean up sample data from finnhub_articles
DELETE
FROM catalog.finnhub_articles
WHERE article_id = 12345;
