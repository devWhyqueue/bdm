# BDM
Code repository for the project in big data management at upc 2025.

# Repository Overview
- We define our DAGs for Airflow in `/dags`
- We separate the development of our tasks in `/tasks` with sub-directories
- The system is containerized with docker in the `docker-compose.yaml`. For starting it, you can go to the airflow webserver on `localhost:8080` or use the cli

# Requirements
- Docker Desktop Installed
- WSL 2 with Ubuntu 22.04 and docker installed (we at least need the `/var/run/docker.sock` file for the DockerOperator)
- API Key for Finazon

# System Setup
- Init Airflow: `sudo docker compose up airflow-init` -> account has username and password `airflow`
- Set the linux access rights for the deamon `sudo usermod -aG docker $USER`
- Need to build all the task containers first, e.g., `docker build ./tasks/stock_stream_src -t stock_stream_kafka_producer_img`
- Run the application `docker compose up`. Access airflow here `localhost:8080` and the kafka ui here `loclhost:8088`
