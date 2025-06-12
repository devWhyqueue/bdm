from __future__ import annotations

import os
import pendulum

from airflow.models.dag import DAG
from airflow.providers.docker.operators.docker import DockerOperator # For Airflow 2.0+
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago

# --------------------------------------------------------------------------------
# Configuration - These should ideally be managed via Airflow Connections / Variables
# --------------------------------------------------------------------------------

# --- PostgreSQL Connection Details ---
# These environment variables are expected by the Docker container running the Spark job.
# Here, we're setting them up to be passed to the DockerOperator.
# In a production setup, use Airflow Variables & Secrets, e.g., Jinja templating:
# POSTGRES_ANALYTICS_DB = "{{ var.value.get('postgres_analytics_db', 'analytics_db') }}"
# For this subtask, we'll use os.getenv to allow docker-compose / .env file to provide them
# during local Airflow testing, with defaults.
POSTGRES_HOST = os.getenv("POSTGRES_HOST_FOR_AIRFLOW", "postgres") # 'postgres' is the service name in docker-compose
POSTGRES_PORT = os.getenv("POSTGRES_PORT_FOR_AIRFLOW", "5432")
POSTGRES_ANALYTICS_DB = os.getenv("POSTGRES_ANALYTICS_DB", "analytics_db")
POSTGRES_ANALYTICS_USER = os.getenv("POSTGRES_ANALYTICS_USER", "analytics_user")
POSTGRES_ANALYTICS_PASSWORD = os.getenv("POSTGRES_ANALYTICS_PASSWORD", "analytics_password")


# --- Docker Configuration ---
# The Docker image name. If docker-compose builds an image like <project>_finnhub-analytics-pipeline, use that.
# Or, add an 'image:' tag to the service in docker-compose.yml and use that tag.
# Assuming the project directory name (parent of bdm) is part of the auto-generated tag.
# This is a common source of needing adjustment.
DOCKER_IMAGE_NAME = os.getenv("FINNHUB_ENRICHMENT_DOCKER_IMAGE", "bdm_finnhub-analytics-pipeline:latest")
# If your docker-compose project is named 'bdm', the image is often 'bdm-finnhub-analytics-pipeline' (with dash)
# or 'bdm_finnhub-analytics-pipeline'. Check `docker images` after `docker-compose build`.
# For robustness, it's best to explicitly tag the image in docker-compose.yml.

# Docker network: Should match the network your postgres and minio services are on.
# If docker-compose project is 'bdm', the network is often 'bdm_default'.
DOCKER_NETWORK_MODE = os.getenv("DOCKER_NETWORK_FOR_AIRFLOW", "bdm_default") # Example: 'yourprojectname_default'

# --- Upstream DAG Details (for ExternalTaskSensor) ---
# These must match the actual DAG ID and Task ID of the Finnhub processing DAG.
UPSTREAM_PROCESSING_DAG_ID = "finnhub_processing_dag" # Placeholder - VERIFY THIS
UPSTREAM_PROCESSING_TASK_ID = "write_silver_finnhub_articles" # Placeholder - VERIFY THIS (or set to None to sense whole DAG)


# --------------------------------------------------------------------------------
# DAG Definition
# --------------------------------------------------------------------------------
with DAG(
    dag_id="finnhub_enrichment_dag",
    start_date=days_ago(1),
    schedule=None, # Intended to be triggered by the ExternalTaskSensor
    catchup=False,
    tags=["finnhub", "enrichment", "analytics", "gold-layer"],
    doc_md="""
    ### Finnhub Articles Enrichment Pipeline (Gold Layer)

    This DAG orchestrates the Spark-based enrichment process for Finnhub articles.
    It is designed to run after the main Finnhub processing DAG (which prepares the
    silver Iceberg table `catalog.finnhub_articles`).

    **Steps:**
    1.  **`wait_for_finnhub_processing`**: Uses `ExternalTaskSensor` to wait for the
        successful completion of the specified task in the upstream Finnhub processing DAG.
    2.  **`run_finnhub_enrichment_pipeline`**: Executes the Dockerized Spark application
        (`finnhub-analytics-pipeline`). This job performs:
        *   Auto-summarization of articles using a Hugging Face model.
        *   Sentiment analysis using VADER.
        *   Schema transformations (column pruning and renaming).
        *   Loading of the enriched data into the `public.finnhub_articles` table
            in the PostgreSQL `analytics_db`.

    **Configuration via Environment Variables (for DockerOperator):**
    - `SPARK_MASTER_URL`: (Defaults to `local[*]` in the container's entrypoint script)
    - `POSTGRES_HOST`: Target PostgreSQL host (service name from Docker network).
    - `POSTGRES_PORT`: Target PostgreSQL port.
    - `POSTGRES_ANALYTICS_DB`: Target database name.
    - `POSTGRES_ANALYTICS_USER`: User for PostgreSQL connection.
    - `POSTGRES_ANALYTICS_PASSWORD`: Password for PostgreSQL connection.
    """,
) as dag:
    wait_for_processing = ExternalTaskSensor(
        task_id="wait_for_finnhub_processing_completion",
        external_dag_id=UPSTREAM_PROCESSING_DAG_ID,
        external_task_id=UPSTREAM_PROCESSING_TASK_ID,
        allowed_states=["success"],
        failed_states=["failed", "skipped", "upstream_failed"],
        mode="poke",
        timeout=60 * 60 * 3,  # Poke for 3 hours
        poke_interval=60 * 2,    # Poke every 2 minutes
        check_existence=True, # Ensures the external DAG/task exists
    )

    run_enrichment_pipeline = DockerOperator(
        task_id="run_finnhub_enrichment_spark_job",
        image=DOCKER_IMAGE_NAME,
        api_version="auto",
        auto_remove="success", # Keeps container on failure for debugging
        command=[], # Uses Docker image's ENTRYPOINT
        docker_url="unix://var/run/docker.sock", # Standard for local Docker setups
        network_mode=DOCKER_NETWORK_MODE,
        environment={
            "SPARK_MASTER_URL": "local[*]", # Explicitly set for clarity, though entrypoint defaults
            "POSTGRES_HOST": POSTGRES_HOST,
            "POSTGRES_PORT": POSTGRES_PORT,
            "POSTGRES_ANALYTICS_DB": POSTGRES_ANALYTICS_DB,
            "POSTGRES_ANALYTICS_USER": POSTGRES_ANALYTICS_USER,
            "POSTGRES_ANALYTICS_PASSWORD": POSTGRES_ANALYTICS_PASSWORD,
            # Set platform to ensure correct image architecture if running on, e.g. Mac M1/M2
            # "DOCKER_DEFAULT_PLATFORM": "linux/amd64", # If needed
        },
        mount_tmp_dir=False, # Avoids issues with tmpfs mounts in some Docker setups
        tty=False, # Ensures logs are captured correctly; Spark UI often uses tty
        # Optional: Add CPU/memory limits if your Airflow setup supports them for DockerOperator
        # mem_limit="4g",
        # cpuset_cpus="0-1",
    )

    wait_for_processing >> run_enrichment_pipeline
