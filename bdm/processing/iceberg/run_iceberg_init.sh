#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

echo "--- Starting Iceberg Initialization Script ---"

# Define MinIO bucket and catalog path details
MINIO_BUCKET_TRUSTED_ZONE="trusted-zone"
ICEBERG_CATALOG_DIR="iceberg_catalog"

echo "--- Ensuring Docker services are up (minio, spark-iceberg) ---"
# This script is now run by a dedicated docker-compose service (iceberg-init-runner)
# which depends on minio and createbuckets.

echo "--- Waiting for dependent services (MinIO, etc.) to be fully ready ---"
# The docker-compose depends_on handles service readiness.
# Adding a small sleep here can still be beneficial for file system operations post bucket creation.
sleep 15 # Increased sleep slightly for more robustness on initial startup

# Define the spark-sql command to be executed
# Spark configurations are loaded from /opt/bitnami/spark/conf/spark-defaults.conf
# The catalog warehouse is defined in 01_create_catalog.sql and also passed here for explicitness.
SPARK_SQL_CMD="spark-sql \
  --conf spark.sql.catalog.catalog.warehouse=s3a://${MINIO_BUCKET_TRUSTED_ZONE}/${ICEBERG_CATALOG_DIR}/"

# Define the MinIO client command (assumes mc is configured in the container or path includes it)
# For docker-compose exec, this was previously: MC_CMD="docker-compose exec -T minio mc"
# Since this script now runs INSIDE a container (iceberg-init-runner, which is spark based),
# we need to execute mc commands differently.
# For now, let's assume mc is NOT in the spark image.
# Verification will rely on spark-sql select and table describe.
# If mc is needed, it would require installing it in the spark image or using docker-in-docker (complex).
# Let's remove direct mc commands from this script when run by iceberg-init-runner.
# Verification of metadata existence can be done manually via `docker-compose exec minio mc ls ...` if needed after script runs.

echo "--- Running 01_create_catalog.sql ---"
$SPARK_SQL_CMD -f /opt/bitnami/spark/work/iceberg_scripts/01_create_catalog.sql
echo "Catalog creation script executed."

echo "--- Running 02_create_reddit_posts_table.sql ---"
$SPARK_SQL_CMD -f /opt/bitnami/spark/work/iceberg_scripts/02_create_reddit_posts_table.sql
echo "Reddit posts table creation script executed."

echo "--- Running 03_create_finnhub_articles_table.sql ---"
$SPARK_SQL_CMD -f /opt/bitnami/spark/work/iceberg_scripts/03_create_finnhub_articles_table.sql
echo "Finnhub articles table creation script executed."

echo "--- Running 04_verify_tables.sql (includes DESCRIBE, INSERT, SELECT) ---"
$SPARK_SQL_CMD -f /opt/bitnami/spark/work/iceberg_scripts/04_verify_tables.sql
echo "Verification script executed."

# Removing direct mc ls commands as they add complexity for containerized execution without mc in spark image.
# The success of spark-sql commands (especially SELECTs in verify_tables.sql) is the primary verification.
# Manual verification can be done as:
# docker-compose exec minio mc ls myminio/trusted-zone/iceberg_catalog/
# docker-compose exec minio mc ls myminio/trusted-zone/iceberg_catalog/catalog/
# docker-compose exec minio mc ls myminio/trusted-zone/iceberg_catalog/catalog/reddit_posts/metadata/
# docker-compose exec minio mc ls myminio/trusted-zone/iceberg_catalog/catalog/finnhub_articles/metadata/
echo "--- MinIO metadata list commands (for manual verification if needed) ---"
echo "docker-compose exec minio mc ls myminio/${MINIO_BUCKET_TRUSTED_ZONE}/${ICEBERG_CATALOG_DIR}/"
echo "docker-compose exec minio mc ls myminio/${MINIO_BUCKET_TRUSTED_ZONE}/${ICEBERG_CATALOG_DIR}/catalog/"
echo "docker-compose exec minio mc ls myminio/${MINIO_BUCKET_TRUSTED_ZONE}/${ICEBERG_CATALOG_DIR}/catalog/reddit_posts/metadata/"
echo "docker-compose exec minio mc ls myminio/${MINIO_BUCKET_TRUSTED_ZONE}/${ICEBERG_CATALOG_DIR}/catalog/finnhub_articles/metadata/"


echo "--- Iceberg Initialization and Verification Completed Successfully ---"
