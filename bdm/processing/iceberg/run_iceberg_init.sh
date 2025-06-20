#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

echo "--- Starting Iceberg Initialization Script ---"

echo "--- Ensuring Docker services are up (minio, spark-iceberg) ---"
MAX_RETRIES=10
RETRY_INTERVAL=10
echo "Checking readiness of dependent services..."
for ((i=1; i<=MAX_RETRIES; i++)); do
    # Check MinIO health via curl
    if curl -s http://minio:9000/minio/health/live >/dev/null; then
        echo "Services are ready."
        break
    fi

    echo "Services not ready yet. Retrying in $RETRY_INTERVAL seconds... ($i/$MAX_RETRIES)"
    sleep "$RETRY_INTERVAL"
done

if (( i > MAX_RETRIES )); then
    echo "Error: Services failed to become ready within the timeout period."
    exit 1
fi


# Define the spark-sql command to be executed
# Spark configurations are loaded from /opt/bitnami/spark/conf/spark-defaults.conf
# The catalog warehouse is defined in 01_create_catalog.sql and also passed here for explicitness.
SPARK_SQL_CMD="spark-sql \
    --conf spark.sql.catalog.iceberg.warehouse=s3a://trusted-zone/iceberg_catalog"

echo "--- Running 01_create_reddit_posts_table.sql ---"
$SPARK_SQL_CMD -f /opt/bitnami/spark/work/iceberg_scripts/01_create_reddit_posts_table.sql
echo "Reddit posts table creation script executed."

echo "--- Running 02_create_finnhub_articles_table.sql ---"
$SPARK_SQL_CMD -f /opt/bitnami/spark/work/iceberg_scripts/02_create_finnhub_articles_table.sql
echo "Finnhub articles table creation script executed."

echo "--- Iceberg Initialization Completed Successfully ---"
