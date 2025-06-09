# Use an official Spark image as a parent image
# Ensure this Spark version is compatible with your Iceberg version and Hadoop libs
FROM bitnami/spark:3.5.0-debian-11-r0

USER root

# Set working directory for the application
WORKDIR /opt/bitnami/spark/work/bdm

# Copy local BDM code (including finnhub processing and utils) into the container
# This assumes the Docker build context is the root of the 'bdm' project
COPY ./ /opt/bitnami/spark/work/bdm/

# Copy requirements file for finnhub processor
COPY ./processing/finnhub/requirements.txt /opt/bitnami/spark/work/bdm/processing/finnhub/requirements.txt

# Install Python dependencies
# Ensure pip is available and up-to-date if necessary
RUN pip install --no-cache-dir -r /opt/bitnami/spark/work/bdm/processing/finnhub/requirements.txt

# Optional: Add any other necessary build steps, like installing other packages or setting permissions

# The entrypoint and cmd will typically be handled by the Spark Docker image
# or overridden by the DockerOperator in Airflow.
# Default command can be set to run spark-submit with the main script if desired for standalone runs.
# For Airflow DockerOperator, the 'command' parameter will specify the spark-submit line.

# Set back to default Spark user if needed, though DockerOperator often runs as root or specified user
USER 1001
