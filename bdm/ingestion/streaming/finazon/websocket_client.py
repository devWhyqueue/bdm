import asyncio
import json
import logging
import os
from typing import Dict, Any

import click
import websockets
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO)

FINAZON_API_URL = f"wss://ws.finazon.io/v1?apikey={os.environ['FINAZON_API_KEY']}"


class FinazonMarketDataProducer:
    """
    A class to connect to Finazon WebSocket API, receive market data and publish it to Kafka.
    """

    def __init__(self, price_ticks_topic: str, stream_topic: str, ticker_symbols: list[str], data_source: str,
                 kafka_endpoint=os.getenv("KAFKA_ENDPOINT")):
        self.price_ticks_topic = price_ticks_topic
        self.stream_topic = stream_topic
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

    async def _send_subscription_request(self, websocket: websockets.WebSocketClientProtocol) -> None:
        """Sends the subscription request to the Finazon WebSocket."""
        subscription_payload = {
            "event": "subscribe",
            "dataset": self.data_source,
            "tickers": self.ticker_symbols,
            "channel": "bars",
            "frequency": "1s",
            "aggregation": "1m"
        }
        await websocket.send(json.dumps(subscription_payload))

    def _transform_market_data(self, market_data: dict) -> dict:
        """Transforms raw market data to the schema for the stream."""
        return {
            "data_source": market_data.get('d', self.data_source),
            "provider": market_data.get('p', 'Finazon'),
            "channel": market_data.get('ch', 'bars'),
            "frequency": market_data.get('f', '1s'),
            "aggregation": market_data.get('aggr', '1m'),
            "symbol": market_data.get('s', ''),
            "timestamp": market_data.get('t', 0),
            "open_price": market_data.get('o', 0.0),
            "close_price": market_data.get('c', 0.0),
            "high_price": market_data.get('h', 0.0),
            "low_price": market_data.get('l', 0.0),
            "volume": market_data.get('v', 0)
        }

    def _send_to_stream(self, transformed_market_data: dict) -> None:
        """Sends transformed market data to the Kafka stream topic."""
        self.kafka_producer.send(
            self.stream_topic,
            json.dumps(transformed_market_data).encode()
        )

    async def _process_message(self, message_str: str) -> None:
        """Processes a single incoming WebSocket message."""
        market_data = json.loads(message_str)
        if not self._validate_market_data(market_data):
            logging.warning("Received incomplete market data: %s", market_data)
            return

        transformed_data = self._transform_market_data(market_data)
        self._send_to_stream(transformed_data)
        await self._send_interpolated_price_ticks(market_data)

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

                # Overwrite all timestamps in market_data with current UTC time (milliseconds)
                import time
                utc_now_ms = time.time_ns() // 1_000_000
                # Use long-form keys for all attributes
                transformed_market_data = {
                    "data_source": market_data.get('d', self.data_source),
                    "provider": market_data.get('p', 'Finazon'),
                    "channel": market_data.get('ch', 'bars'),
                    "frequency": market_data.get('f', '1s'),
                    "aggregation": market_data.get('aggr', '1m'),
                    "symbol": market_data.get('s', ''),
                    "timestamp": utc_now_ms,
                    "open_price": market_data.get('o', 0.0),
                    "close_price": market_data.get('c', 0.0),
                    "high_price": market_data.get('h', 0.0),
                    "low_price": market_data.get('l', 0.0),
                    "volume": market_data.get('v', 0)
                }
                # Send full event to volume stream (every second)
                self.kafka_producer.send(
                    self.stream_topic,
                    json.dumps(transformed_market_data).encode()
                )
                # Send interpolated price ticks to price ticks topic
                await self._send_interpolated_price_ticks(transformed_market_data)

    @staticmethod
    def _validate_market_data(market_data: Dict[str, Any]) -> bool:
        """Validate that the market data contains all required fields."""
        required_fields = ['o', 'c', 'h', 'l', 'v', 't', 's']
        return all(field in market_data for field in required_fields)

    async def _send_interpolated_price_ticks(self, market_data: Dict[str, Any]) -> None:
        """Interpolates 90 price points between open and close prices and sends to Kafka price ticks topic with event time."""
        open_price = market_data['open_price']
        close_price = market_data['close_price']
        price_step = (close_price - open_price) / 90
        base_timestamp = market_data.get('timestamp', 0)
        interval_ms = 1000 // 90  # Spread ticks evenly over 1 second
        for i in range(90):
            interpolated_price = round(open_price + i * price_step, 5)
            tick_timestamp = base_timestamp + i * interval_ms
            message = {
                "symbol": market_data.get('symbol', ''),
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
@click.option('--stream-topic', required=True, help='Kafka topic for full market data (warm path).')
@click.option('--tickers', required=True, help='Comma-separated list of ticker symbols to subscribe to.')
@click.option('--dataset', required=True, help='Dataset identifier to subscribe to (e.g., us_stocks_essential).')
def main(price_ticks_topic, stream_topic, tickers, dataset):
    """Command line interface to start the Finazon market data producer."""
    ticker_symbols_list = tickers.split(',')
    producer = FinazonMarketDataProducer(price_ticks_topic, stream_topic, ticker_symbols_list, dataset)
    producer.run()


if __name__ == "__main__":
    main()
