"""
### Reddit Trusted Zone Processing DAG (TaskFlow API)
This DAG orchestrates the processing of Reddit data from the landing zone
to the trusted zone using a Spark job running in Docker.
It dynamically determines the input path based on the execution date.
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag
from airflow.models.param import Param
from airflow.providers.docker.operators.docker import DockerOperator

# Define the default Docker image name. This can be overridden by a DAG parameter.
DEFAULT_DOCKER_IMAGE_NAME = "reddit-data-processor:latest"

# Path to the main Python script inside the Docker image
# Assumes the Docker image is built with the bdm code at this location.
SPARK_APP_PYTHON_FILE = "/opt/bitnami/spark/work/bdm/processing/reddit/process_reddit_data.py"

env_vars = {
    'MINIO_ENDPOINT': '{{ conn.s3.extra_dejson.host }}',
    'MINIO_ACCESS_KEY': '{{ conn.s3.login }}',
    'MINIO_SECRET_KEY': '{{ conn.s3.password }}',
    'MINIO_BUCKET': 'trusted-zone', # This is for the Iceberg table, not media.
    'PROCESSED_ZONE_BUCKET': '{{ params.processed_zone_bucket }}',
}

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": pendulum.duration(minutes=5),
}


@dag(
    dag_id="reddit_processing",
    default_args=default_args,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # Changed from "@daily" to None
    catchup=False,
    tags=["reddit", "processing", "trusted-zone", "spark", "docker", "taskflow"],
    doc_md=__doc__,
    params={
        "input_subreddit": Param(
            "bitcoin",
            type="string",
            title="Subreddit Name",
            description="The subreddit to process data for (e.g., 'bitcoin', 'wallstreetbets')."
        ),
        "landing_zone_bucket": Param(
            "landing-zone",
            type="string",
            title="Landing Zone Bucket",
            description="S3/MinIO bucket name for the landing zone."
        ),
        "docker_image": Param(
            DEFAULT_DOCKER_IMAGE_NAME,
            type="string",
            title="Docker Image",
            description="Docker image for the Spark Reddit processor (e.g., 'myimage:tag')."
        ),
        "processed_zone_bucket": Param(
            "processed-zone", # Default value
            type="string",
            title="Processed Zone Bucket",
            description="S3/MinIO bucket name for storing processed media files."
        ),
    }
)
def reddit_processing_dag():
    """
    ### Reddit Data Processing DAG using TaskFlow API

    This DAG orchestrates a Spark job running in a Docker container to process Reddit data.
    It features:
    - Dynamic input path generation based on execution date and subreddit.
    - Configuration via DAG Parameters (subreddit, bucket, Docker image).
    - Secure credential management for MinIO via Airflow Connections.
    """

    input_path_template = "s3a://{{ params.landing_zone_bucket }}/{{ dag_run.conf.input_filename }}"

    DockerOperator(
        task_id="run_reddit_spark_processor",
        image="{{ params.docker_image }}",
        api_version="auto",  # Use client's API version
        auto_remove="force",  # Remove container after execution
        command=["--input-path", input_path_template],
        docker_url="unix://var/run/docker.sock",  # Standard Docker socket
        network_mode="bdm_default",  # Changed from bridge
        environment=env_vars,
        mount_tmp_dir=False,  # Usually not needed if Spark uses its own temp dirs
        doc_md="""
        #### Run Spark Reddit Processor Task
        This task executes the main Spark application (`process_reddit_data.py`)
        within a Docker container. It processes Reddit data from a specified
        S3 input path (dynamically generated) and writes to an Iceberg table.
        
        - **Docker Image**: `{{ params.docker_image }}`
        - **Input Path Template**: `{input_path_template}`
        - **MinIO Connection**: `{MINIO_CONN_ID}`
        """,
    )


# Instantiate the DAG to make it discoverable by Airflow
reddit_processing_dag_instance = reddit_processing_dag()
