# Use the official Airflow image as base
FROM apache/airflow:2.10.5

USER root

RUN apt-get update && apt-get install -y build-essential libffi-dev libpq-dev \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir --use-deprecated=legacy-resolver \
    apache-airflow-providers-amazon \
    apache-airflow-providers-apache-kafka \
    apache-airflow-providers-docker \
    openmetadata-ingestion[postgres,kafka,datalake-s3,openlineage]==1.7.3.0 \
    openmetadata-managed-apis==1.7.3.0 \
 && rm -rf /airflow/.cache/pip

