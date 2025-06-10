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

# Copy local BDM code (including finnhub processing and utils) into the container
# This assumes the Docker build context is the root of the 'bdm' project
COPY architecture/spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf
COPY ./bdm /app/bdm

# Copy requirements file for finnhub processor
COPY ./bdm/processing/finnhub/requirements.txt /app/requirements.txt

# Install Python dependencies
# Ensure pip is available and up-to-date if necessary
RUN pip install --no-cache-dir -r /app/requirements.txt

ENV PYTHONPATH=/app
ENV SPARK_USER=spark

ENTRYPOINT ["spark-submit",  "--conf", "spark.jars.ivy=/tmp/.ivy2",  "/app/bdm/processing/finnhub/process_finnhub_data.py"]
