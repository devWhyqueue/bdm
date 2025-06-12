from __future__ import annotations

import json
import re

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.docker.operators.docker import DockerOperator

DEFAULT_DOCKER_IMAGE_NAME = "finnhub-data-processor:latest"

env_vars = {
    'MINIO_ENDPOINT': '{{ conn.s3.extra_dejson.host }}',
    'MINIO_ACCESS_KEY': '{{ conn.s3.login }}',
    'MINIO_SECRET_KEY': '{{ conn.s3.password }}',
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
Input path is provided via `dag_run.conf.input_filename` and `dag_run.conf.landing_zone_bucket`.
"""

@dag(
    dag_id="finnhub_processing",
    default_args=default_args,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # Triggered DAG
    catchup=False,
    tags=["finnhub", "processing", "trusted-zone", "spark", "docker"],
    doc_md=dag_doc_md,
    params={
        "docker_image": Param(
            DEFAULT_DOCKER_IMAGE_NAME,
            type="string",
            title="Docker Image",
            description="Docker image for the Spark Finnhub processor."
        ),
    }
)
def finnhub_processing_dag():
    # Construct input_path from dag_run.conf if available, otherwise params might be used as a fallback for manual runs
    # For triggered runs, dag_run.conf.input_filename and dag_run.conf.landing_zone_bucket are expected.
    input_file_path = (
        "s3a://{{ dag_run.conf.get('landing_zone_bucket', 'landing-zone') }}/"
        "{{ dag_run.conf.get('input_filename', '') }}"
    )

    task_doc_md = f"""
    #### Run Spark Finnhub Processor Task
    Executes the Spark application.
    - **Docker Image**: `{{{{ params.docker_image }}}}`
    - **Input Path**: `{input_file_path}` (Dynamically set from trigger)
    """

    command = [
        "--input-path", input_file_path
    ]

    run_spark_processor = DockerOperator(
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
        do_xcom_push=True,
        xcom_all=True
    )

    @task
    def extract_scraped_at_from_logs(task_instance=None):
        log_data = task_instance.xcom_pull(task_ids='run_finnhub_spark_processor')
        if not isinstance(log_data, list):
            return {}

        # Pre-compile the pattern once
        pattern = re.compile(r"XCOM_PUSH:(\{[^}]*'latest_scraped_at'[^}]*\})")

        # Iterate in reverse so the first match is the latest
        for entry in reversed(log_data):
            if not entry:
                continue
            text = str(entry)
            m = pattern.search(text)
            if m:
                raw = m.group(1).replace("'", '"')
                try:
                    payload = json.loads(raw)
                    ts = payload.get('latest_scraped_at')
                    if ts:
                        return {"latest_scraped_at": ts}
                except json.JSONDecodeError:
                    # Could log warning here if desired
                    break

        return {}

    extract_timestamp = extract_scraped_at_from_logs()

    # Pass the scraped_at parameter to the enrichment DAG
    trigger_enrichment_dag = TriggerDagRunOperator(
        task_id="trigger_finnhub_enrichment_dag",
        trigger_dag_id="finnhub_enrichment_dag",
        conf={
            "latest_scraped_at": "{{ task_instance.xcom_pull(task_ids='extract_scraped_at_from_logs')['latest_scraped_at'] }}"},
        wait_for_completion=False,
        deferrable=False,
    )

    run_spark_processor >> extract_timestamp >> trigger_enrichment_dag

finnhub_processing_dag_instance = finnhub_processing_dag()
