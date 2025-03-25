FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy script
COPY subreddit_scraper.py .

# Set entrypoint
ENTRYPOINT ["python", "subreddit_scraper.py"]

# Default command (can be overridden)
CMD []
