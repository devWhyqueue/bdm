#!/bin/bash
set -eu
# -----------------------------------------------------------------------------
# 01_init-finnhub-articles.sh
#
# Assumes that 00_init-analytics-db.sh has already:
#   • created $POSTGRES_ANALYTICS_DB
#   • created $POSTGRES_ANALYTICS_USER and $POSTGRES_ANALYTICS_PASSWORD
#
# Environment variables expected (all already present in the official image):
#   POSTGRES_ANALYTICS_DB
#   POSTGRES_ANALYTICS_USER
#   POSTGRES_ANALYTICS_PASSWORD
#   (optional) PGHOST, PGPORT – default to unix socket / localhost if omitted
# -----------------------------------------------------------------------------

export PGPASSWORD="${POSTGRES_ANALYTICS_PASSWORD:?Need POSTGRES_ANALYTICS_PASSWORD}"

psql -v ON_ERROR_STOP=1 \
     --username "${POSTGRES_ANALYTICS_USER}" \
     --dbname   "${POSTGRES_ANALYTICS_DB}" <<-EOSQL
    /* -----------------------------------------------------------------------
       Create finnhub_articles table (idempotent) and supporting objects
    ----------------------------------------------------------------------- */
    CREATE TABLE IF NOT EXISTS public.finnhub_articles (
        id               BIGINT PRIMARY KEY,
        category         VARCHAR(255) NOT NULL,
        published_at     TIMESTAMP    NOT NULL,
        headline         TEXT         NOT NULL,
        source           VARCHAR(255) NOT NULL,
        summary          TEXT,
        url              VARCHAR(2048) NOT NULL,
        image_url        VARCHAR(2048),
        scraped_at       TIMESTAMP    NOT NULL,
        sentiment_score  FLOAT,
        sentiment_label  VARCHAR(10)
            CHECK (sentiment_label IN ('positive', 'neutral', 'negative'))
    );

    /* Ensure the user can work in the public schema now and in the future. */
    GRANT USAGE ON SCHEMA public TO ${POSTGRES_ANALYTICS_USER};
    GRANT SELECT, INSERT, UPDATE, DELETE
          ON ALL TABLES IN SCHEMA public
          TO ${POSTGRES_ANALYTICS_USER};
    ALTER DEFAULT PRIVILEGES IN SCHEMA public
          GRANT SELECT, INSERT, UPDATE, DELETE
          ON TABLES TO ${POSTGRES_ANALYTICS_USER};

    /* Helpful indexes for common access patterns. */
    CREATE INDEX IF NOT EXISTS idx_finnhub_articles_published_at
        ON public.finnhub_articles (published_at);

    CREATE INDEX IF NOT EXISTS idx_finnhub_articles_source
        ON public.finnhub_articles (source);

    CREATE INDEX IF NOT EXISTS idx_finnhub_articles_sentiment_label
        ON public.finnhub_articles (sentiment_label);
EOSQL

echo "Finnhub table created and privileges set in '${POSTGRES_ANALYTICS_DB}' for user '${POSTGRES_ANALYTICS_USER}'."
