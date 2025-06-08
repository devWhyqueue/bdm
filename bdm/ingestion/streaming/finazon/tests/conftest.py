"""
Pytest configuration file for Finazon WebSocket client tests.
"""
from unittest import mock

import pytest
from flask.cli import load_dotenv

load_dotenv()


@pytest.fixture
def mock_kafka_producer():
    """Creates a mock for the KafkaProducer."""
    with mock.patch('bdm.finnhub.streaming.finazon.websocket_client.KafkaProducer') as mock_producer_class:
        # Create mock producer
        mock_producer = mock.Mock()
        mock_producer_class.return_value = mock_producer
        yield mock_producer


@pytest.fixture
def mock_websocket_connect():
    """Creates a mock for websockets.connect."""
    with mock.patch('websockets.connect') as mock_connect:
        # Create a mock websocket connection
        mock_websocket = mock.AsyncMock()

        # Configure the mock to return itself in the async context manager
        mock_connect.return_value.__aenter__.return_value = mock_websocket

        # Return both the connect function and the websocket object for assertions
        yield mock_connect, mock_websocket


@pytest.fixture
def mock_sleep():
    """Creates a mock for asyncio.sleep."""
    with mock.patch('asyncio.sleep', autospec=True) as mock_sleep:
        # Configure the mock to return immediately
        mock_sleep.return_value = None
        yield mock_sleep


@pytest.fixture
def sample_market_data():
    """Returns sample market data from Finazon."""
    return {
        "d": "us_stocks_essential",
        "p": "finazon",
        "ch": "bars",
        "f": "1s",
        "aggr": "1m",
        "s": "AAPL",
        "t": 1699540020,
        "o": 220.06,
        "h": 220.13,
        "l": 219.92,
        "c": 219.96,
        "v": 4572
    }


@pytest.fixture
def expected_processed_data():
    """Returns the expected processed market data."""
    return {
        "symbol": "AAPL",
        "timestamp": 1699540020,
        "price": 220.06
    }
