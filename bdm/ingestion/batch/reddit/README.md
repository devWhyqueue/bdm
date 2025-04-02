# Reddit Scraper

This component ingests data from Reddit subreddits, including text content and media files (images, GIFs, and videos).
It saves the data to a MinIO/S3 compatible storage service.

## Features

- Scrapes posts from specified subreddits
- Downloads and stores media files (images, GIFs, videos)
- Handles Reddit gallery posts (multiple images)
- Organizes media in S3 by type (images, videos, etc.)
- Configurable via environment variables or command-line options

## Usage

```bash
# Basic usage (scrapes r/bitcoin by default)
python subreddit_scraper.py

# Scrape a specific subreddit
python subreddit_scraper.py --subreddit wallstreetbets

# Specify sort order, limit, and time filter
python subreddit_scraper.py --subreddit pics --sort-by top --time-filter day --limit 50

# Disable media downloads
python subreddit_scraper.py --subreddit pics --skip-media
```

## Environment Variables

| Variable               | Description                                                           | Default     |
|------------------------|-----------------------------------------------------------------------|-------------|
| `REDDIT_CLIENT_ID`     | Reddit API client ID                                                  | (required)  |
| `REDDIT_CLIENT_SECRET` | Reddit API client secret                                              | (required)  |
| `REDDIT_USER_AGENT`    | Reddit API user agent                                                 | (required)  |
| `SUBREDDIT`            | Subreddit to scrape                                                   | bitcoin     |
| `SORT_BY`              | How to sort posts (new, hot, top, rising, controversial)              | new         |
| `TIME_FILTER`          | Time filter for top/controversial (hour, day, week, month, year, all) | all         |
| `POST_LIMIT`           | Maximum number of posts to scrape                                     | 25          |
| `MINIO_ENDPOINT`       | MinIO/S3 endpoint URL                                                 | (required)  |
| `MINIO_ACCESS_KEY`     | MinIO/S3 access key                                                   | (required)  |
| `MINIO_SECRET_KEY`     | MinIO/S3 secret key                                                   | (required)  |
| `MINIO_BUCKET`         | MinIO/S3 bucket for data storage                                      | reddit-data |
| `OBJECT_PREFIX`        | Prefix for S3 object keys                                             | (empty)     |
| `SKIP_MEDIA`           | Skip downloading media if set to "true"                               | (empty)     |

## Data Format

### JSON Structure

Posts are saved as JSON files with the following schema:

```json
{
  "metadata": {
    "subreddit": "pics",
    "sort_by": "hot",
    "time_filter": null,
    "post_count": 25,
    "posts_with_media": 15,
    "total_media_items": 20,
    "scraped_at": "2023-07-20T10:15:30.123456"
  },
  "posts": [
    {
      "id": "post_id",
      "title": "Post title",
      "author": "username",
      "created_utc": 1626782130.0,
      "score": 1500,
      "url": "https://example.com/image.jpg",
      "selftext": "Post content...",
      "num_comments": 123,
      "permalink": "/r/pics/comments/post_id/title/",
      "upvote_ratio": 0.95,
      "is_original_content": false,
      "is_self": false,
      "media": [
        {
          "media_type": "image",
          "s3_url": "reddit/pics/images/abc123.jpg",
          "source_url": "https://example.com/image.jpg",
          "filename": "abc123.jpg",
          "content_type": "image/jpeg"
        }
      ]
    }
  ]
}
```

### Media Storage

Media files are stored in S3 with the following structure:

```
s3://<bucket>/reddit/<subreddit>/images/
s3://<bucket>/reddit/<subreddit>/videos/
s3://<bucket>/reddit/<subreddit>/gifs/
```

Each media file has a unique UUID-based filename with the appropriate file extension.
