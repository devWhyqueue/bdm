FROM bitnami/spark:3.5.1

# Set working directory
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

# Copy application files
COPY architecture/spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf
COPY ./bdm /app/bdm
COPY ./bdm/analytics/finnhub/requirements.txt /app/requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Set entrypoint
ENTRYPOINT ["spark-submit", "--conf", "spark.jars.ivy=/tmp/.ivy2", "/app/bdm/analytics/finnhub/run_enrichment.py"]
CMD []