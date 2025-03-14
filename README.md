# bdm

# Requirements
- Docker Desktop Installed
- WSL 2 with Ubuntu 22.04
- API Key for Finazon

# Airflow Setup
- Need to build the task container first: `docker build ./tasks/stock_stream_src -t stock_stream_kafka_producer_img`
- Init Airflow: `docker compose up airflow-init` -> account has username and password `airflow`
- Docker deamon access for DockerOperator # TODO