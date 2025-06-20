# BDM

Big data project for the BDM course at UPC.

## Structure

```
├── architecture/       # Docker Compose files for the architecture
├── bdm/                # Source code for the project
│   ├── airflow/        # Airflow DAGs for the project
│   ├── ingestion/      # Landing zone
│       ├── batch/      # Batch ingestion containers
│       ├── streaming/  # Streaming ingestion containers
│   ├── processing/     # Trusted zone
│       ├── iceberg/    # Creation of Iceberg tables
│       ├── reddit/     # Reddit processing containers
│       ├── finnhub/    # Finnhub processing containers
│   ├── analytics/      # Analytics zone
│       ├── finazon/    # Finazon analytics container
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
- **Flink UI**: [http://localhost/flink](http://localhost/flink)
- **MinIO UI**: [http://localhost:9001](http://localhost:9001)
- **InfluxDB UI**: [http://localhost:8086](http://localhost:8086)
- **Grafana**: [http://localhost:3000](http://localhost:3000)

### Accessing Databases

**PostgreSQL (Consolidated Instance)**

- **Host**: `localhost`
- **Port**: `5432`

This single PostgreSQL instance now hosts multiple databases. You can connect to it using any standard PostgreSQL client
or GUI tool (e.g., pgAdmin, DBeaver, DataGrip).

1. **Airflow Database:**
    * **User**: `airflow`
    * **Password**: `airflow`
    * **Database Name**: `airflow`

2. **Exploitation Zone Database (Analytics):**
    * **User**: `analytics_user`
    * **Password**: `analytics_password` (as set in `architecture/database.yml`)
    * **Database Name**: `analytics_db`

**MongoDB (`mongodb`)**

- **Host**: `localhost`
- **Port**: `27017`
- Typically does not require authentication by default for local setups.

**InfluxDB**

- **Host**: `localhost`
- **Port**: `8086`
- **User**: `admin` (see `DOCKER_INFLUXDB_INIT_USERNAME` in `architecture/influxdb.yml`)
- **Password**: `adminpassword` (see `DOCKER_INFLUXDB_INIT_PASSWORD`)
- **Bucket**: `exploitation_zone_streaming_data` (see `DOCKER_INFLUXDB_INIT_BUCKET`)
- Default credentials and bucket setup can be found in `architecture/influxdb.yml`.
- Used for time-series data from streaming jobs (e.g., price ticks, VWAP).

### Grafana

- **Grafana** service is now included for real-time visualization of both streams:
    - Real-time stock price (hot-path) graph: price vs. time, grouped by symbol.
    - VWAP (warm-path) graph: VWAP vs. time, grouped by symbol.
    - VWAP stream details (symbol, data_source) are displayed as stat panels.
    - Access Grafana at http://localhost:3000 (default: admin/admin).
    - InfluxDB is preconfigured as a data source; dashboard is auto-provisioned.
    - See `architecture/grafana/` for provisioning and dashboard files.

To stop the project, run:

```sh
docker compose down
```
