from __future__ import annotations

import pendulum
from airflow.decorators import dag
from airflow.models.param import Param
from airflow.providers.docker.operators.docker import DockerOperator

DEFAULT_DOCKER_IMAGE_NAME = "finnhub-data-processor:latest"
SPARK_APP_PYTHON_FILE = "/opt/bitnami/spark/work/bdm/processing/finnhub/process_finnhub_data.py"

env_vars = {
    'MINIO_ENDPOINT': '{{ conn.s3.extra_dejson.host }}',
    'MINIO_ACCESS_KEY': '{{ conn.s3.login }}',
    'MINIO_SECRET_KEY': '{{ conn.s3.password }}',
    'MINIO_LANDING_BUCKET': 'landing-zone',
    'MINIO_TRUSTED_BUCKET': 'trusted-zone',
}

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": pendulum.duration(minutes=5),
}

dag_doc_md = """
### Finnhub Trusted Zone Processing DAG
Orchestrates the processing of Finnhub news data from the landing zone
to the trusted zone using a Spark job running in Docker.
Input path should be templated.
Example: `s3a://landing-zone/finnhub/{{ execution_date.strftime('%Y/%m/%d') }}/*.json`
The actual input path is provided via `dag_run.conf.input_path_glob`.
"""

@dag(
    dag_id="finnhub_processing",
    default_args=default_args,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["finnhub", "processing", "trusted-zone", "spark", "docker"],
    doc_md=dag_doc_md,
    params={
        "input_path_glob": Param(
            "s3a://landing-zone/finnhub/YYYY/MM/DD/*.json",
            type="string",
            title="Input Path Glob",
            description="S3 glob pattern for input Finnhub JSON files."
        ),
        "docker_image": Param(
            DEFAULT_DOCKER_IMAGE_NAME,
            type="string",
            title="Docker Image",
            description="Docker image for the Spark Finnhub processor."
        ),
    }
)
def finnhub_processing_dag():
    task_doc_md = """
    #### Run Spark Finnhub Processor Task
    Executes the `process_finnhub_data.py` Spark application.
    - **Docker Image**: `{{ params.docker_image }}`
    - **Input Path**: `{{ params.input_path_glob }}`
    """

    command = [
        SPARK_APP_PYTHON_FILE,
        "--input-path", "{{ params.input_path_glob }}"
    ]

    DockerOperator(
        task_id="run_finnhub_spark_processor",
        image="{{ params.docker_image }}",
        api_version="auto",
        auto_remove="force",
        command=command,
        docker_url="unix://var/run/docker.sock",
        network_mode="bdm_default",
        environment=env_vars,
        mount_tmp_dir=False,
        doc_md=task_doc_md,
    )

finnhub_processing_dag_instance = finnhub_processing_dag()
