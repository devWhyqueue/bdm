FROM bitnami/spark:3.5.6

# Set working directory for the application
WORKDIR /app

# Copy local BDM code (including finnhub processing and utils) into the container
# This assumes the Docker build context is the root of the 'bdm' project
COPY ./bdm /app/bdm

# Copy requirements file for finnhub processor
COPY ./bdm/processing/finnhub/requirements.txt /app/requirements.txt

# Install Python dependencies
# Ensure pip is available and up-to-date if necessary
RUN pip install --no-cache-dir -r app/requirements.txt

ENV PYTHONPATH=/app

ENTRYPOINT ["spark-submit", "/app/bdm/processing/finnhub/process_finnhub_data.py"]
