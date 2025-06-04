import asyncio
import json
import logging
import os
import sys
from typing import Dict, Any

import click
import websockets
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO)

if 'pytest' in sys.modules:
    FINAZON_API_KEY = os.environ.get('FINAZON_API_KEY', '')
else:
    FINAZON_API_KEY = os.environ.get('FINAZON_API_KEY')
    if not FINAZON_API_KEY:
        raise RuntimeError("FINAZON_API_KEY environment variable must be set for Finazon API access.")
FINAZON_API_URL = f"wss://ws.finazon.io/v1?apikey={FINAZON_API_KEY}"


class FinazonMarketDataProducer:
    """
    A class to connect to Finazon WebSocket API, receive market data and publish it to Kafka.
    """

    def __init__(self, price_ticks_topic: str, volume_stream_topic: str, ticker_symbols: list[str], data_source: str,
                 kafka_endpoint=os.getenv("KAFKA_ENDPOINT")):
        self.price_ticks_topic = price_ticks_topic
        self.volume_stream_topic = volume_stream_topic
        self.ticker_symbols = ticker_symbols
        self.data_source = data_source
        self.kafka_producer = KafkaProducer(bootstrap_servers=[kafka_endpoint])

    def run(self):
        """Start the market data producer."""
        try:
            asyncio.run(self._process_market_data())
        except KeyboardInterrupt:
            logging.info("Application shutdown requested")
        finally:
            self.kafka_producer.close()

    async def _process_market_data(self) -> None:
        """Process market data with retry logic for connection issues."""
        reconnect_delay_seconds = 10
        max_reconnect_attempts = 3
        reconnect_attempts = 0

        while reconnect_attempts < max_reconnect_attempts:
            try:
                await self._handle_websocket_connection()
                reconnect_attempts = 0
            except Exception as error:
                logging.error("WebSocket connection error: %s", error)
                reconnect_attempts += 1
                if reconnect_attempts >= max_reconnect_attempts:
                    logging.error("Maximum reconnection attempts reached, exiting.")
                    raise error
                await asyncio.sleep(reconnect_delay_seconds)

    async def _handle_websocket_connection(self) -> None:
        """Establish WebSocket connection and process incoming market data messages."""
        logging.info("Establishing WebSocket connection to Finazon API")
        async with websockets.connect(FINAZON_API_URL, ping_interval=30, ping_timeout=60) as websocket:
            await websocket.send(json.dumps({
                "event": "subscribe",
                "dataset": self.data_source,
                "tickers": self.ticker_symbols,
                "channel": "bars",
                "frequency": "1s",
                "aggregation": "1m"
            }))

            # Process incoming messages
            async for message in websocket:
                market_data = json.loads(message)
                if not self._validate_market_data(market_data):
                    logging.warning("Received incomplete market data: %s", market_data)
                    continue

                # Send full event to volume stream (every second)
                self.kafka_producer.send(
                    self.volume_stream_topic,
                    json.dumps(market_data).encode()
                )

                # Send interpolated price ticks to price ticks topic
                await self._send_interpolated_price_ticks(market_data)

    @staticmethod
    def _validate_market_data(market_data: Dict[str, Any]) -> bool:
        """Validate that the market data contains all required fields."""
        required_fields = ['o', 'c', 'h', 'l', 'v', 't', 's']
        return all(field in market_data for field in required_fields)

    async def _send_interpolated_price_ticks(self, market_data: Dict[str, Any]) -> None:
        """Interpolates 90 price points between open and close prices and sends to Kafka price ticks topic with event time."""
        open_price = market_data['o']
        close_price = market_data['c']
        price_step = (close_price - open_price) / 90
        base_timestamp = market_data.get('t', 0)
        interval_ms = 1000 // 90  # Spread ticks evenly over 1 second

        for i in range(90):
            interpolated_price = round(open_price + i * price_step, 5)
            tick_timestamp = base_timestamp + i * interval_ms
            message = {
                "symbol": market_data.get('s', ''),
                "timestamp": tick_timestamp,
                "price": interpolated_price
            }
            self.kafka_producer.send(
                self.price_ticks_topic,
                json.dumps(message).encode()
            )
            await asyncio.sleep(0.01)


@click.command()
@click.option('--price-ticks-topic', required=True, help='Kafka topic for high-frequency price ticks (hot path).')
@click.option('--volume-stream-topic', required=True, help='Kafka topic for full market data (warm path).')
@click.option('--tickers', required=True, help='Comma-separated list of ticker symbols to subscribe to.')
@click.option('--dataset', required=True, help='Dataset identifier to subscribe to (e.g., us_stocks_essential).')
def main(price_ticks_topic, volume_stream_topic, tickers, dataset):
    """Command line interface to start the Finazon market data producer."""
    ticker_symbols_list = tickers.split(',')
    producer = FinazonMarketDataProducer(price_ticks_topic, volume_stream_topic, ticker_symbols_list, dataset)
    producer.run()


if __name__ == "__main__":
    main()
