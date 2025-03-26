# Bitcoin Tick Aggregator with Apache Flink

This module implements a real-time aggregation pipeline for Bitcoin trade ticks using Apache Flink. It consumes
high-frequency Bitcoin tick data from Kafka, performs time-window based aggregations, and stores the results in MinIO (
S3-compatible storage).

## Architecture

The Bitcoin Tick Aggregator consists of the following components:

1. **Kafka Source**: Consumes Bitcoin tick data from the Kafka topic `btc_usdt`.
2. **Flink Processing**: Performs window-based aggregations (1-minute windows) on the tick data.
3. **MinIO Sink**: Stores the aggregated results in the MinIO `landing-zone` bucket.

## Aggregation Metrics

For each 1-minute window, the following metrics are calculated:

- **Average Price**: The mean price of all ticks in the window.
- **Median Price**: The median price of all ticks in the window.
- **Min Price**: The minimum price observed in the window.
- **Max Price**: The maximum price observed in the window.
- **Trade Count**: The number of trades/ticks in the window.

## Output Format

The aggregated data is stored in JSON format with the following structure:

```json
{
  "timestamp": "2025-03-25T13:30:00",
  "window_start": "2025-03-25T13:29:00",
  "window_end": "2025-03-25T13:30:00",
  "avg_price": 63245.75,
  "median_price": 63247.50,
  "min_price": 63240.25,
  "max_price": 63251.00,
  "trade_count": 6000
}
```

## Data Partitioning

The output data in MinIO is partitioned by date (YYYY-MM-DD) to facilitate efficient querying and processing of
historical data.

## Fault Tolerance

The Flink job is configured with checkpointing to ensure fault tolerance. If the job fails, it can be restarted and will
continue processing from the last successful checkpoint.

## Monitoring

The Flink UI is available at `/flink` in the web interface, allowing you to monitor the job's performance, throughput,
and any errors that may occur.

## Environment Variables

The following environment variables are required:

- `KAFKA_ENDPOINT`: The Kafka bootstrap servers (e.g., `kafka:9092`).
- `KAFKA_TOPIC`: The Kafka topic to consume from (e.g., `btc_usdt`).
- `MINIO_ENDPOINT`: The MinIO endpoint URL (e.g., `http://minio:9000`).
- `MINIO_ACCESS_KEY`: The MinIO access key.
- `MINIO_SECRET_KEY`: The MinIO secret key.
- `MINIO_BUCKET`: The MinIO bucket to store aggregated data (e.g., `landing-zone`).
