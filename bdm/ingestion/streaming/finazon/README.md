# Finazon Market Data Streaming Module

A Python module for streaming market data from Finazon's WebSocket API to Kafka.

## Features

- Connect to Finazon's WebSocket API to receive real-time financial data
- Subscribe to specific tickers (e.g., AAPL, MSFT) and data sources
- Process and transform market data with descriptive field names
- Interpolate 90 price points between the open and close prices
- Publish interpolated data to Kafka for downstream processing
- Auto-reconnect on connection errors with configurable retry parameters

## Prerequisites

- Python 3.8+
- Access to a Kafka broker
- Finazon API key (set as environment variable)

## Installation

```bash
pip install -r requirements.txt
```

For development and testing:

```bash
pip install -r requirements-dev.txt
```

## Configuration

The following environment variables need to be set:

- `FINAZON_API_KEY`: Your Finazon API key
- `KAFKA_ENDPOINT`: Kafka broker endpoint (default: localhost:9092)

## Usage

### Command Line

```bash
python -m bdm.ingestion.streaming.finazon.websocket_client \
  --topic market_data \
  --tickers AAPL,MSFT,GOOGL \
  --dataset us_stocks_essential
```

### As a Library

```python
from bdm.ingestion.streaming.finazon.websocket_client import FinazonMarketDataProducer

# Initialize the producer
producer = FinazonMarketDataProducer(
    kafka_topic="market_data",
    ticker_symbols=["AAPL", "MSFT", "GOOGL"],
    data_source="us_stocks_essential"
)

# Start receiving and publishing data
producer.run()
```

## Data Format

The module processes Finazon WebSocket data and interpolates 90 price points between the open and close prices. Each
resulting message is formatted with descriptive field names:

```json
{
  "data_source": "us_stocks_essential",
  "provider": "finazon",
  "channel": "bars",
  "frequency": "1s",
  "aggregation": "1m",
  "symbol": "AAPL",
  "timestamp": 1699540020,
  "high_price": 220.13,
  "low_price": 219.92,
  "volume": 4572,
  "price": 220.04
}
```

The module will generate 90 messages for each market data update received from Finazon, with the price gradually
changing from the open price to the close price.

## Testing

Run the tests with:

```bash
pytest bdm/ingestion/streaming/finazon/tests/
```

## License

This project is licensed under the terms of the company's license.

## Acknowledgements

- [Finazon API](https://finazon.io/) - Financial data provider
- [Kafka](https://kafka.apache.org/) - Distributed streaming platform 
