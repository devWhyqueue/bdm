"""
Unit tests for Finazon WebSocket client functionality.
"""
import json
from unittest import mock

import pytest

from bdm.ingestion.streaming.finazon.websocket_client import (
    FinazonMarketDataProducer,
    main
)


class TestFinazonMarketDataProducer:
    """Tests for the FinazonMarketDataProducer class."""

    @pytest.fixture
    def producer(self, mock_kafka_producer):
        """Create a producer instance for testing."""
        return FinazonMarketDataProducer(
            price_ticks_topic="test_price_ticks",
            volume_stream_topic="test_volume_stream",
            ticker_symbols=["AAPL", "MSFT"],
            data_source="us_stocks_essential"
        )

    @staticmethod
    def test_init(producer, mock_kafka_producer):
        """Test initialization of the producer."""
        assert producer.price_ticks_topic == "test_price_ticks"
        assert producer.volume_stream_topic == "test_volume_stream"
        assert producer.ticker_symbols == ["AAPL", "MSFT"]
        assert producer.data_source == "us_stocks_essential"
        assert producer.kafka_producer is mock_kafka_producer

    @staticmethod
    def test_validate_market_data_valid(producer, sample_market_data):
        """Test validation with valid market data."""
        assert FinazonMarketDataProducer._validate_market_data(sample_market_data) is True

    @staticmethod
    def test_validate_market_data_invalid(producer):
        """Test validation with invalid market data."""
        # Missing required fields
        invalid_data = {"d": "us_stocks_essential", "p": "finazon"}
        assert FinazonMarketDataProducer._validate_market_data(invalid_data) is False

        # Missing one required field
        partial_data = {
            "d": "us_stocks_essential",
            "p": "finazon",
            "ch": "bars",
            "s": "AAPL",
            "t": 1699540020,
            "o": 220.06,
            "h": 220.13,
            # Missing 'l' field
            "c": 219.96,
            "v": 4572
        }
        assert FinazonMarketDataProducer._validate_market_data(partial_data) is False

    @pytest.mark.asyncio
    async def test_send_interpolated_price_ticks(self, producer, sample_market_data, mock_sleep):
        """Test interpolated price ticks sending."""
        # Call the method
        await producer._send_interpolated_price_ticks(sample_market_data)

        # Assert the data was sent to Kafka 90 times
        assert producer.kafka_producer.send.call_count == 90

        # Check the first interpolated message
        first_call_args = producer.kafka_producer.send.call_args_list[0][0]
        first_message = json.loads(first_call_args[1].decode())

        # Topic should be correct
        assert first_call_args[0] == "test_price_ticks"

        # Message should contain expected data
        assert first_message["symbol"] == "AAPL"
        assert first_message["timestamp"] == 1699540020
        assert abs(first_message["price"] - 220.06) < 0.01

        # Check last message to ensure interpolation reached close price
        last_call_args = producer.kafka_producer.send.call_args_list[89][0]
        last_message = json.loads(last_call_args[1].decode())

        # Last price should be close to close price
        assert abs(last_message["price"] - 219.96) < 0.01

        # Sleep should have been called 90 times to control the rate
        assert mock_sleep.call_count == 90

    @pytest.mark.asyncio
    async def test_handle_websocket_connection(self, producer, mock_websocket_connect, sample_market_data):
        """Test handling WebSocket connection and processing messages."""
        _, mock_websocket = mock_websocket_connect

        # Configure the mock to provide a message then raise an exception to exit the loop
        mock_websocket.__aiter__.return_value = [json.dumps(sample_market_data)]

        # Mock the validation and processing methods
        with mock.patch.object(FinazonMarketDataProducer, '_validate_market_data', return_value=True) as mock_validate, \
                mock.patch.object(producer, '_send_interpolated_price_ticks') as mock_process:
            await producer._handle_websocket_connection()

            # Assert the subscription was sent
            subscription_message = json.dumps({
                "event": "subscribe",
                "dataset": "us_stocks_essential",
                "tickers": ["AAPL", "MSFT"],
                "channel": "bars",
                "frequency": "1s",
                "aggregation": "1m"
            })
            mock_websocket.send.assert_called_once_with(subscription_message)

            # Assert the market data was validated
            mock_validate.assert_called_once_with(sample_market_data)

            # Assert the market data was processed
            mock_process.assert_called_once_with(sample_market_data)

    @pytest.mark.asyncio
    async def test_handle_websocket_invalid_data(self, producer, mock_websocket_connect):
        """Test handling invalid WebSocket data."""
        _, mock_websocket = mock_websocket_connect

        # Configure the mock to provide an invalid message
        invalid_data = {"d": "us_stocks_essential"}
        mock_websocket.__aiter__.return_value = [json.dumps(invalid_data)]

        # Mock the validation method to return False and the processing method
        with mock.patch.object(FinazonMarketDataProducer, '_validate_market_data', return_value=False) as mock_validate, \
                mock.patch.object(producer, '_send_interpolated_price_ticks') as mock_process, \
                mock.patch('logging.warning') as mock_logging:
            await producer._handle_websocket_connection()

            # Assert the validation was called but processing was not
            mock_validate.assert_called_once_with(invalid_data)
            mock_process.assert_not_called()

            # Assert a warning was logged
            mock_logging.assert_called_once()

    @staticmethod
    def test_run(producer):
        """Test the run method."""
        # Create a regular Mock (not AsyncMock) to avoid coroutine warnings
        mock_process = mock.Mock()

        # Mock asyncio.run and the close method
        with mock.patch.object(producer, '_process_market_data', mock_process), \
                mock.patch('asyncio.run') as mock_run, \
                mock.patch.object(producer.kafka_producer, 'close') as mock_close:
            producer.run()

            # Assert asyncio.run was called once (don't check the exact argument)
            assert mock_run.call_count == 1

            # Assert kafka producer was closed
            mock_close.assert_called_once()

    @staticmethod
    def test_run_with_keyboard_interrupt(producer):
        """Test the run method with keyboard interrupt."""
        # We'll patch asyncio.run but test the real run method with a mock for _process_market_data
        # Create a mock for _process_market_data that's not a coroutine
        mock_process = mock.Mock()

        def side_effect(*args, **kwargs):
            raise KeyboardInterrupt()

        with mock.patch('asyncio.run', side_effect=side_effect), \
                mock.patch.object(producer, '_process_market_data', mock_process), \
                mock.patch.object(producer.kafka_producer, 'close') as mock_close, \
                mock.patch('logging.info') as mock_logging:
            producer.run()

            # Assert that asyncio.run was attempted to be called with our mocked method
            # Assert shutdown message was logged
            mock_logging.assert_called_once_with("Application shutdown requested")

            # Assert kafka producer was closed
            mock_close.assert_called_once()


class TestCommandLineInterface:
    """Tests for the command line interface."""

    @staticmethod
    def test_main():
        with mock.patch(
                'bdm.ingestion.streaming.finazon.websocket_client.FinazonMarketDataProducer') as mock_producer_class:
            mock_producer = mock.Mock()
            mock_producer_class.return_value = mock_producer
            # Simulate Click CLI call using CliRunner
            from click.testing import CliRunner
            runner = CliRunner()
            result = runner.invoke(
                main,
                [
                    '--price-ticks-topic', 'test_price_ticks',
                    '--volume-stream-topic', 'test_volume_stream',
                    '--tickers', 'AAPL,MSFT',
                    '--dataset', 'us_stocks_essential'
                ]
            )
            assert result.exit_code == 0
            mock_producer_class.assert_called_once_with(
                'test_price_ticks',
                'test_volume_stream',
                ['AAPL', 'MSFT'],
                'us_stocks_essential'
            )
            mock_producer.run.assert_called_once()
