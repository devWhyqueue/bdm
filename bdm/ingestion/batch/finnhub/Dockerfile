FROM python:3.12-slim

WORKDIR /app

# Copy project
COPY bdm/ ./bdm/

# Install dependencies
RUN pip install --no-cache-dir -r bdm/ingestion/batch/finnhub/requirements.txt

# Set PYTHONPATH so Python can resolve the package
ENV PYTHONPATH=/app

# Run the script
ENTRYPOINT ["python", "bdm/ingestion/batch/finnhub/news_scraper.py"]
