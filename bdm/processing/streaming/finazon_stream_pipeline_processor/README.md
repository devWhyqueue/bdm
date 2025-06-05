# Finazon Stream Pipeline Processor (Flink)

A Flink streaming job to join hot (price ticks) and warm (full market data) Kafka streams, measure latency, compute VWAP, and write results to InfluxDB.

## Features
- Consumes hot and warm Kafka topics
- Joins streams using Flink windowing
- Computes latency and VWAP
- Outputs to InfluxDB (to be implemented)

## Build

```
mvn clean package
```

## Run (Docker)

```
docker build -t finazon-stream-pipeline-processor .
# Example Flink run command (adjust as needed):
# docker run --network=host finazon-stream-pipeline-processor
```

## Usage

Build the JAR:

```
mvn clean package
```

Build the Docker image:

```
docker build -t finazon-stream-pipeline-processor .
```

Run the Flink job in Docker (replace values as needed):

```
docker run --network=host finazon-stream-pipeline-processor <price_ticks_topic> <volume_stream_topic> <kafka_bootstrap_servers> <influxdb_port>
```

- `<price_ticks_topic>`: Kafka topic for high-frequency price ticks (hot path)
- `<volume_stream_topic>`: Kafka topic for full market data (warm path)
- `<kafka_bootstrap_servers>`: Kafka broker address (e.g., `kafka:9092`)
- `<influxdb_port>`: InfluxDB port (e.g., `8086`)

**Example:**

```
docker run --network=host finazon-stream-pipeline-processor price_ticks volume_stream kafka:9092 8086
```

You can also use these arguments in your `docker-compose.yml` to configure the service at runtime.

## TODO
- Implement stream join and VWAP logic
- Add InfluxDB sink
- Add configuration options
