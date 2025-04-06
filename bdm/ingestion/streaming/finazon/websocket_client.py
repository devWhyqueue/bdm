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

    def __init__(self, kafka_topic: str, ticker_symbols: list[str], data_source: str,
                 kafka_endpoint=os.getenv("KAFKA_ENDPOINT")):
        self.kafka_topic = kafka_topic
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

                await self._send_interpolated_data(market_data)

    @staticmethod
    def _validate_market_data(market_data: Dict[str, Any]) -> bool:
        """Validate that the market data contains all required fields."""
        required_fields = ['o', 'c', 'h', 'l', 'v', 't', 's']
        return all(field in market_data for field in required_fields)

    async def _send_interpolated_data(self, market_data: Dict[str, Any]) -> None:
        """Interpolates 90 price points between open and close prices and sends to Kafka."""
        open_price = market_data['o']
        close_price = market_data['c']
        price_step = (close_price - open_price) / 90

        # Create a well-structured base message with descriptive field names
        base_message = {
            "data_source": market_data.get('d', ''),
            "provider": market_data.get('p', ''),
            "channel": market_data.get('ch', ''),
            "frequency": market_data.get('f', ''),
            "aggregation": market_data.get('aggr', ''),
            "symbol": market_data.get('s', ''),
            "timestamp": market_data.get('t', 0),
            "high_price": market_data.get('h', 0.0),
            "low_price": market_data.get('l', 0.0),
            "volume": market_data.get('v', 0)
        }

        # Send 90 interpolated price points
        for i in range(90):
            # Calculate the interpolated price for this step
            interpolated_price = round(open_price + i * price_step, 5)

            # Create a message with the interpolated price
            message = base_message.copy()
            message["price"] = interpolated_price

            # Send to Kafka
            self.kafka_producer.send(
                self.kafka_topic,
                json.dumps(message).encode()
            )

            # Small delay to prevent overwhelming the broker
            await asyncio.sleep(0.01)


@click.command()
@click.option('--topic', required=True, help='Kafka topic to send market data to.')
@click.option('--tickers', required=True, help='Comma-separated list of ticker symbols to subscribe to.')
@click.option('--dataset', required=True, help='Dataset identifier to subscribe to (e.g., us_stocks_essential).')
def main(topic, tickers, dataset):
    """Command line interface to start the Finazon market data producer."""
    ticker_symbols_list = tickers.split(',')
    producer = FinazonMarketDataProducer(topic, ticker_symbols_list, dataset)
    producer.run()


if __name__ == "__main__":
    main()
