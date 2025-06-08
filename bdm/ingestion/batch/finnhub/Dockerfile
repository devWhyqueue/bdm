FROM python:3.12-slim
WORKDIR /app

# 1) Copy the inner bdm folder (your context) into /app/bdm
COPY . /app/bdm

# 2) Install requirements from the newly-copied path
RUN pip install --no-cache-dir \
      -r /app/bdm/ingestion/batch/finnhub/requirements.txt

# 3) Make /app the import root, so 'import bdm...' works
ENV PYTHONPATH=/app

# 4) Invoke as a module
ENTRYPOINT ["python", "-m", "bdm.ingestion.batch.finnhub.news_scraper"]
