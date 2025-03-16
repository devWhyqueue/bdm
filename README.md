# bdm

# Requirements
- Docker Desktop Installed
- WSL 2 with Ubuntu 22.04 and docker installed (!) -> give docker correct permissions for port # TODO
- API Key for Finazon

# Airflow Setup
- Open WSL and run all commands there
- Need to build the task container first: `docker build ./tasks/stock_stream_src -t stock_stream_kafka_producer_img`
- Init Airflow: `sudo docker compose up airflow-init` -> account has username and password `airflow`
- Docker deamon access for DockerOperator # TODO

- `sudo usermod -aG docker $USER` `sudo chmod 666 /var/run/docker.sock`
