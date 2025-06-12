-- This script is expected to run in the analytics_db database
-- The init-analytics-db.sh script should have already created analytics_db and analytics_user

CREATE TABLE IF NOT EXISTS public.finnhub_articles (
    id BIGINT PRIMARY KEY,
    category VARCHAR(255) NOT NULL,
    published_at TIMESTAMP NOT NULL, -- Renamed from published_at_utc
    headline TEXT NOT NULL,
    source VARCHAR(255) NOT NULL,
    summary TEXT, -- Will be enriched
    url VARCHAR(2048) NOT NULL,
    image_url VARCHAR(2048),
    scraped_at TIMESTAMP NOT NULL,
    sentiment_score FLOAT,
    sentiment_label VARCHAR(10) CHECK (sentiment_label IN ('positive', 'neutral', 'negative'))
);

ALTER TABLE public.finnhub_articles OWNER TO "${POSTGRES_ANALYTICS_USER}";

CREATE INDEX IF NOT EXISTS idx_finnhub_articles_published_at ON public.finnhub_articles (published_at);
CREATE INDEX IF NOT EXISTS idx_finnhub_articles_source ON public.finnhub_articles (source);
CREATE INDEX IF NOT EXISTS idx_finnhub_articles_sentiment_label ON public.finnhub_articles (sentiment_label);
