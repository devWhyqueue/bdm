# Finazon Stream Pipeline Processor (Flink)

A Flink streaming job to join hot (price ticks) and warm (full market data) Kafka streams, measure latency, compute VWAP, and write results to InfluxDB.

## Features
- Consumes hot and warm Kafka topics
- Joins streams using Flink windowing
- Computes latency and VWAP
- Outputs to InfluxDB (to be implemented)

## Build & Deploy

This project uses a multi-stage Docker build. The JAR is built automatically during the Docker image build—no manual Maven step is required.

### 1. Build the Docker image

```
docker compose build finazon-stream-pipeline-processor
```

### 2. Start the Flink cluster and dependencies

```
docker compose up -d jobmanager taskmanager kafka minio
```

### 3. Submit the Flink job

Copy the JAR and submit script to the jobmanager container and run the script:

```
docker cp bdm/processing/streaming/finazon_stream_pipeline_processor/target/finazon-stream-pipeline-processor*.jar jobmanager:/opt/flink/usrlib/
docker cp bdm/processing/streaming/finazon_stream_pipeline_processor/submit-job.sh jobmanager:/opt/flink/usrlib/
docker exec jobmanager chmod +x /opt/flink/usrlib/submit-job.sh
docker exec jobmanager /opt/flink/usrlib/submit-job.sh
```

### 4. Environment Variables

Set these environment variables in your docker-compose.yml or before submitting the job:
- `PRICE_TICKS_TOPIC` (e.g. price_ticks)
- `VOLUME_STREAM_TOPIC` (e.g. volume_stream)
- `KAFKA_ENDPOINT` (e.g. kafka:9092)
- `INFLUXDB_PORT` (e.g. 8086)

### 5. Monitoring

- Use the Flink UI at http://localhost:8081 to monitor job status and logs.

---

No manual `mvn clean package` step is needed—the build is fully automated in Docker. The JAR is placed in `/opt/flink/usrlib` for Flink compatibility, matching the structure of other streaming jobs in this repository. Job submission is handled via the included `submit-job.sh` script.

## Usage

All configuration is now passed via environment variables. Set the following variables when running the container:

- `PRICE_TICKS_TOPIC`: Kafka topic for high-frequency price ticks (hot path)
- `VOLUME_STREAM_TOPIC`: Kafka topic for full market data (warm path)
- `KAFKA_ENDPOINT`: Kafka broker address (e.g., `kafka:9092`)
- `INFLUXDB_PORT`: InfluxDB port (e.g., `8086`)

**Example Docker run:**

```
docker run --network=host \
  -e PRICE_TICKS_TOPIC=price_ticks \
  -e VOLUME_STREAM_TOPIC=volume_stream \
  -e KAFKA_ENDPOINT=kafka:9092 \
  -e INFLUXDB_PORT=8086 \
  finazon-stream-pipeline-processor
```

**Example docker-compose.yml snippet:**

```
  finazon-stream-pipeline-processor:
    build:
      context: bdm/processing/streaming/finazon_stream_pipeline_processor
      dockerfile: Dockerfile
    container_name: finazon-stream-pipeline-processor
    environment:
      - PRICE_TICKS_TOPIC=price_ticks
      - VOLUME_STREAM_TOPIC=volume_stream
      - KAFKA_ENDPOINT=kafka:9092
      - INFLUXDB_PORT=8086
```

No command-line arguments are required or supported; all configuration is via environment variables.
