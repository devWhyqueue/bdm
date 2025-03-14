# BDM

Big data project for the BDM course at UPC.

## Structure

```
/
├── airflow/
│   ├── dags/           # DAGs for the project
├── architecture/       # Docker Compose files for the architecture
├── docker-compose.yml  # Main Docker Compose file
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

## Running the Project

To start the project, run:

```sh
docker compose -p bdm up -d --build
```

Once the services are up, you can access:

- **Airflow UI**: [http://localhost/airflow](http://localhost/airflow)
- **Kafka UI**: [http://localhost/kafka](http://localhost/kafka)

To stop the project, run:

```sh
docker compose down
```

