# Use Bitnami Spark base image as requested
FROM bitnami/spark:3.5.6-debian-12 AS spark-base

# Set working directory
WORKDIR /opt/bitnami/spark/app

# Copy application files
COPY ./run_enrichment.py ./run_enrichment.py
COPY ./requirements.txt ./requirements.txt

# Install Python dependencies
# Bitnami images run as non-root (user 1001).
# Install system-wide as root, then switch back to default user.
USER root

# Install pip if not present or to ensure system pip, then install requirements and nltk data
RUN apt-get update && apt-get install -y --no-install-recommends python3-pip && \
    pip3 install --no-cache-dir -r requirements.txt && \
    python3 -m nltk.downloader punkt && \
    apt-get purge -y --auto-remove python3-pip && \
    rm -rf /var/lib/apt/lists/*

# Switch back to the default non-root user provided by Bitnami Spark image
USER 1001

# Copy entrypoint script (will be created in the next step)
COPY ./start-spark-job.sh ./start-spark-job.sh
RUN chmod +x ./start-spark-job.sh

# Set entrypoint
ENTRYPOINT ["./start-spark-job.sh"]
