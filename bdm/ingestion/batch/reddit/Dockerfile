FROM python:3.12-slim

WORKDIR /app

# Copy project
COPY bdm/ ./bdm/

# Install dependencies
RUN pip install --no-cache-dir -r bdm/ingestion/batch/reddit/requirements.txt

# Set PYTHONPATH so Python can resolve the package
ENV PYTHONPATH=/app

# Set entrypoint
ENTRYPOINT ["python", "bdm/ingestion/batch/reddit/subreddit_scraper.py"]

# Default command (can be overridden)
CMD []
