FROM python:3.12-slim

# 1) Set a working dir for everything that follows
WORKDIR /app

# 2) Copy your entire `bdm` package (the context) into /app/bdm
COPY . /app/bdm

# 3) Install only your Reddit-scraper dependencies
RUN pip install --no-cache-dir \
      -r /app/bdm/ingestion/batch/reddit/requirements.txt

# 4) Ensure Python can import `bdm.*`
ENV PYTHONPATH=/app

# 5) Invoke the scraper as a module so imports like `from bdm...` work
ENTRYPOINT ["python", "-m", "bdm.ingestion.batch.reddit.subreddit_scraper"]

# 6) Allow extra flags via CMD (e.g. "--help")
CMD []
