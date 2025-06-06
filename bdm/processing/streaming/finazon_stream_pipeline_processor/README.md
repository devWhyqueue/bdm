# Finazon Stream Pipeline Processor (Flink)

A Flink streaming job that joins hot (price ticks, "hot-path") and warm (full market data, "warm-path") Kafka streams, measures latency, computes VWAP, and writes results to InfluxDB.

## Features
- Consumes hot-path (price ticks) and warm-path (full market data) Kafka topics
- Hot-path: validates price ticks (price >= 0), measures event latency, and writes each tick to InfluxDB
- Warm-path: computes per-second volume from cumulative volume ticks
- Joins hot and warm streams by symbol, computes 1-minute windowed VWAP, and writes VWAP results to InfluxDB

## Quickstart

### Build & Run
1. **Build Docker image:**
   ```
   docker compose build finazon-stream-pipeline-processor
   ```
2. **Start services:**
   ```
   docker compose up -d jobmanager taskmanager kafka minio influxdb
   ```
3. **Submit the Flink job:**
   ```
   docker cp bdm/processing/streaming/finazon_stream_pipeline_processor/target/finazon-stream-pipeline-processor*.jar jobmanager:/opt/flink/usrlib/
   docker cp bdm/processing/streaming/finazon_stream_pipeline_processor/submit-job.sh jobmanager:/opt/flink/usrlib/
   docker exec jobmanager chmod +x /opt/flink/usrlib/submit-job.sh
   docker exec jobmanager /opt/flink/usrlib/submit-job.sh
   ```

## Configuration
Set these environment variables (in `docker-compose.yml` or when running the container):
- `PRICE_TICKS_TOPIC`: Kafka topic for price ticks (hot-path)
- `STREAM_TOPIC`: Kafka topic for full market data (warm-path)
- `KAFKA_ENDPOINT`: Kafka broker (e.g. kafka:9092)
- `INFLUXDB_HOST`: InfluxDB host (e.g. influxdb)
- `INFLUXDB_PORT`: InfluxDB port (e.g. 8086)
- `INFLUXDB_BUCKET`: InfluxDB bucket (e.g. exploitation_zone_streaming_data)
- `INFLUXDB_ORG`: InfluxDB org (e.g. bdm)
- `INFLUXDB_TOKEN`: InfluxDB API token (write access)

**InfluxDB:**
- The bucket is auto-created with 1h retention (see `architecture/influxdb.yml`).
- VWAP is written to `vwap_1min`, price ticks to `hot_path_ticks` in InfluxDB.

## Monitoring
- Flink UI: http://localhost:8081
- InfluxDB UI: http://localhost:8086

---
No manual Maven build is needed; Docker handles everything. All configuration is via environment variables. See `architecture/influxdb.yml` for InfluxDB setup details.

