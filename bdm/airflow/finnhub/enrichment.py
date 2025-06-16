from __future__ import annotations

import os

from airflow.models.dag import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago

POSTGRES_HOST = os.getenv("POSTGRES_HOST_FOR_AIRFLOW", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT_FOR_AIRFLOW", "5432")
POSTGRES_ANALYTICS_DB = os.getenv("POSTGRES_ANALYTICS_DB", "analytics_db")
POSTGRES_ANALYTICS_USER = os.getenv("POSTGRES_ANALYTICS_USER", "analytics_user")
POSTGRES_ANALYTICS_PASSWORD = os.getenv("POSTGRES_ANALYTICS_PASSWORD", "analytics_password")

DOCKER_IMAGE_NAME = os.getenv("FINNHUB_ENRICHMENT_DOCKER_IMAGE", "finnhub-analytics-pipeline:latest")
DOCKER_NETWORK_MODE = os.getenv("DOCKER_NETWORK_FOR_AIRFLOW", "bdm_default")

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
    
    Parameters:
    * `latest_scraped_at` - Optional ISO timestamp to filter data in the silver layer
    """,
) as dag:
    # Get latest_scraped_at from the upstream DAG or default to None
    latest_scraped_at = "{{ dag_run.conf.get('latest_scraped_at', '') }}"

    # Build command array with the filter if available
    command = []
    if "{{ dag_run.conf.get('latest_scraped_at', '') }}" != "":
        command.extend(["--filter-scraped-at", "{{ dag_run.conf.get('latest_scraped_at', '') }}"])

    run_enrichment_pipeline = DockerOperator(
        task_id="run_finnhub_enrichment_spark_job",
        image=DOCKER_IMAGE_NAME,
        api_version="auto",
        auto_remove="success", # Keeps container on failure for debugging
        command=command,  # Pass the scraped_at filter if available
        docker_url="unix://var/run/docker.sock", # Standard for local Docker setups
        network_mode=DOCKER_NETWORK_MODE,
        environment={
            "POSTGRES_HOST": POSTGRES_HOST,
            "POSTGRES_PORT": POSTGRES_PORT,
            "POSTGRES_ANALYTICS_DB": POSTGRES_ANALYTICS_DB,
            "POSTGRES_ANALYTICS_USER": POSTGRES_ANALYTICS_USER,
            "POSTGRES_ANALYTICS_PASSWORD": POSTGRES_ANALYTICS_PASSWORD,
        },
        mount_tmp_dir=False,
        tty=False,
    )
