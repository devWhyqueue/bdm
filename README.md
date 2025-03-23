# BDM

Big data project for the BDM course at UPC.

## Structure

```
├── architecture/       # Docker Compose files for the architecture
├── bdm/                # Source code for the project
│   ├── airflow/        # Airflow configuration
│       ├── dags/       # DAGs for the project
│   ├── ingestion/      
│       ├── batch/      # Batch ingestion containers
│       ├── streaming/  # Streaming ingestion containers
├── docker-compose.yml  # Main Docker Compose file
|── .env                # Environment variables for the project
├── pyproject.toml      # Dependencies for local development
```

## Requirements

Before running the project, ensure you have the following installed:

- [Docker](https://docs.docker.com/get-docker/)
- [Poetry](https://python-poetry.org/docs/#installation)

## Development

To install dependencies for local development:

```sh
poetry install
```

## Environment variables

To run the project, you need to set the following environment variables in the `.env` file:

- `FINAZON_API_KEY`: API key for the Finazon API
- `AIRFLOW_CONN_REDDIT_API`: Connection ID for the Reddit API in Airflow

## Running the Project

To start the project, run:

```sh
docker compose -p bdm up -d --build
```

Once the services are up, you can access:

- **Airflow UI**: [http://localhost/airflow](http://localhost/airflow)
- **Kafka UI**: [http://localhost/kafka](http://localhost/kafka)
- **MinIO UI**: [http://localhost:9001](http://localhost:9001)

To stop the project, run:

```sh
docker compose down
```

