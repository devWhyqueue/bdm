#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

echo "--- Starting Iceberg Initialization Script ---"

# Define MinIO bucket and catalog path details
MINIO_BUCKET_TRUSTED_ZONE="trusted-zone"
ICEBERG_CATALOG_DIR="iceberg_catalog"

echo "--- Ensuring Docker services are up (minio, spark-iceberg) ---"

echo "--- Waiting for dependent services (MinIO, etc.) to be fully ready ---"
# The docker-compose depends_on handles service readiness.
# Adding a small sleep here can still be beneficial for file system operations post bucket creation.
sleep 15

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

echo "--- Running 03_verify_tables.sql (includes DESCRIBE, INSERT, SELECT) ---"
$SPARK_SQL_CMD -f /opt/bitnami/spark/work/iceberg_scripts/03_verify_tables.sql
echo "Verification script executed."

echo "--- Iceberg Initialization and Verification Completed Successfully ---"
