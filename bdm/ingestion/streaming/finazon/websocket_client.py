import asyncio
import json
import logging
import os
from typing import Dict, Any

import click
import websockets
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO)

FINAZON_API = f"wss://ws.finazon.io/v1?apikey={os.environ['FINAZON_API_KEY']}"


class AssetTickProducer:
    def __init__(self, topic: str, tickers: list[str], dataset: str, kafka_endpoint=os.getenv("KAFKA_ENDPOINT")):
        self.topic = topic
        self.tickers = tickers
        self.dataset = dataset
        self.producer = KafkaProducer(bootstrap_servers=[kafka_endpoint])

    def run(self):
        try:
            asyncio.run(self._process_stock_data())
        except KeyboardInterrupt:
            logging.info("Application shutdown requested")
        finally:
            self.producer.close()

    async def _process_stock_data(self) -> None:
        reconnect_delay = 10
        max_retries = 3
        retries = 0
        while retries < max_retries:
            try:
                await self._handle_websocket_connection()
                retries = 0
            except Exception as e:
                logging.error("WebSocket error: %s", e)
                retries += 1
                if retries >= max_retries:
                    logging.error("Max retries reached, failing hard.")
                    raise e
                await asyncio.sleep(reconnect_delay)

    async def _handle_websocket_connection(self) -> None:
        """Establish WebSocket connection and process messages."""
        logging.info("Establishing WebSocket connection")
        async with websockets.connect(FINAZON_API, ping_interval=30, ping_timeout=60) as ws:
            await ws.send(json.dumps({
                "event": "subscribe",
                "dataset": self.dataset,
                "tickers": self.tickers,
                "channel": "bars",
                "frequency": "1s",
                "aggregation": "1m"
            }))

            # Process incoming messages
            async for message in ws:
                data = json.loads(message)
                if 'o' not in data or 'c' not in data:
                    logging.warning("Received message: %s", data)
                    continue

                await self._send_interpolated_data_async(data)

    async def _send_interpolated_data_async(self, data: Dict[str, Any]) -> None:
        """Interpolates 90 messages asynchronously, ensuring no overlap."""
        open_price = data['o']
        close_price = data['c']
        step = (close_price - open_price) / 90

        for i in range(90):
            price = round(open_price + i * step, 5)
            self.producer.send(self.topic, json.dumps({"p": price}).encode())
            await asyncio.sleep(0.01)


@click.command()
@click.option('--topic', help='Kafka topic to send data to.')
@click.option('--tickers', help='Tickers to subscribe to.')
@click.option('--dataset', help='Dataset to subscribe to.')
def main(topic, tickers, dataset):
    tickers_list = tickers.split(',')
    producer = AssetTickProducer(topic, tickers_list, dataset)
    producer.run()


if __name__ == "__main__":
    main()
