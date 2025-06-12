#!/bin/bash
set -e # Exit immediately if a command exits with a non-zero status.

# Variables
APP_NAME="FinnhubAnalyticsEnrichment"
SPARK_APPLICATION_PYTHON_LOCATION="/opt/bitnami/spark/app/run_enrichment.py"

# Spark Master URL: Use environment variable or default to local mode
SPARK_MASTER_URL="${SPARK_MASTER_URL:-local[*]}}"

echo "─────────────────────────────────────────────────────────────────"
echo " Starting Spark Application: ${APP_NAME}"
echo "─────────────────────────────────────────────────────────────────"
echo "Spark Master URL:          ${SPARK_MASTER_URL}"
echo "Application Script:        ${SPARK_APPLICATION_PYTHON_LOCATION}"
echo "PostgreSQL Host:           ${POSTGRES_HOST}"
echo "PostgreSQL Port:           ${POSTGRES_PORT}"
echo "PostgreSQL Database:       ${POSTGRES_ANALYTICS_DB}"
echo "PostgreSQL User:           ${POSTGRES_ANALYTICS_USER}"
# Note: POSTGRES_ANALYTICS_PASSWORD is intentionally not echoed for security.
echo "─────────────────────────────────────────────────────────────────"


# Ensure that critical environment variables for the python script are set
if [[ -z "${POSTGRES_HOST}" || -z "${POSTGRES_PORT}" || -z "${POSTGRES_ANALYTICS_DB}" || -z "${POSTGRES_ANALYTICS_USER}" || -z "${POSTGRES_ANALYTICS_PASSWORD}" ]]; then
  echo "[ERROR] Critical PostgreSQL connection environment variables are missing!"
  echo "Please set: POSTGRES_HOST, POSTGRES_PORT, POSTGRES_ANALYTICS_DB, POSTGRES_ANALYTICS_USER, POSTGRES_ANALYTICS_PASSWORD"
  exit 1
fi

# Execute spark-submit
# The Bitnami Spark image sets up SPARK_HOME and other necessary Spark environment variables.
# Configurations from spark-defaults.conf (e.g., JDBC jars, Iceberg settings) should be picked up automatically.

exec $SPARK_HOME/bin/spark-submit \
  --master "${SPARK_MASTER_URL}" \
  --name "${APP_NAME}" \
  --deploy-mode client \
  "${SPARK_APPLICATION_PYTHON_LOCATION}"
  # Example additional configurations (uncomment and adjust if needed):
  # --driver-memory 1g \
  # --executor-memory 1g \
  # --num-executors 1 \
  # --executor-cores 1 \
  # --conf spark.sql.shuffle.partitions=2 \

# The 'exec' command replaces the shell process with the spark-submit process.
# If spark-submit finishes, the container will exit.
# So, any commands after 'exec' (like the 'echo "Spark application ... finished."' from my draft) will not run.
