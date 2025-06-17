FROM bitnami/spark:3.5.6

# Set working directory for the application
WORKDIR /app

USER root

# Create a real spark user with UID=1001 and home directory
RUN \
  useradd -m -u 1001 -s /bin/bash spark && \
  mkdir -p /tmp/.ivy2 && \
  chown spark:spark /tmp/.ivy2

# Switch to the newly created spark user
USER spark
ENV HOME=/home/spark

COPY architecture/spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf

# 1) Copy the inner bdm folder (your context) into /app/bdm
COPY ./bdm /app/bdm

# 2) Install requirements from the newly-copied path
RUN pip install --no-cache-dir \
      -r /app/bdm/ingestion/batch/finnhub/requirements.txt

# 3) Make /app the import root, so 'import bdm...' works
ENV PYTHONPATH=/app
ENV SPARK_USER=spark

ENTRYPOINT ["spark-submit",  "--conf", "spark.jars.ivy=/tmp/.ivy2",  "/app/bdm/ingestion/batch/finnhub/news_scraper.py"]
