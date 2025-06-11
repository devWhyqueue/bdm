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
            stream_topic="test_volume_stream",
            ticker_symbols=["AAPL", "MSFT"],
            data_source="us_stocks_essential"
        )

    @pytest.fixture
    def transformed_market_data(self, sample_market_data):
        """Provides a sample of transformed market data."""
        producer = FinazonMarketDataProducer(
            price_ticks_topic="",
            stream_topic="",
            ticker_symbols=[],
            data_source="us_stocks_essential"
        )
        return producer._transform_market_data(sample_market_data)

    @staticmethod
    def test_init(producer, mock_kafka_producer):
        """Test initialization of the producer."""
        assert producer.price_ticks_topic == "test_price_ticks"
        assert producer.stream_topic == "test_volume_stream"
        assert producer.ticker_symbols == ["AAPL", "MSFT"]
        assert producer.data_source == "us_stocks_essential"
        assert producer.kafka_producer is mock_kafka_producer

    @staticmethod
    def test_validate_market_data_valid(producer, sample_market_data):
        """Test validation with valid market data."""
        assert FinazonMarketDataProducer._validate_market_data(sample_market_data) is True

    @staticmethod
    def test_validate_market_data_invalid(producer, sample_market_data):
        """Test validation with invalid market data."""
        assert FinazonMarketDataProducer._validate_market_data({}) is False

        required_fields = ['o', 'c', 'h', 'l', 'v', 't', 's']
        for field in required_fields:
            invalid_data = sample_market_data.copy()
            del invalid_data[field]
            assert FinazonMarketDataProducer._validate_market_data(invalid_data) is False

    @staticmethod
    def test_transform_market_data(producer, sample_market_data, transformed_market_data):
        """Test the transformation of raw market data."""
        result = producer._transform_market_data(sample_market_data)
        assert result == transformed_market_data

    @pytest.mark.asyncio
    async def test_send_interpolated_price_ticks(self, producer, transformed_market_data, mock_sleep):
        """Test interpolated price ticks sending."""
        await producer._send_interpolated_price_ticks(transformed_market_data)

        assert producer.kafka_producer.send.call_count == 90

        first_call_args = producer.kafka_producer.send.call_args_list[0][0]
        first_message = json.loads(first_call_args[1].decode())

        assert first_call_args[0] == "test_price_ticks"
        assert first_message["symbol"] == transformed_market_data["symbol"]
        assert first_message["timestamp"] == transformed_market_data["timestamp"]
        assert abs(first_message["price"] - transformed_market_data["open_price"]) < 0.01

        last_call_args = producer.kafka_producer.send.call_args_list[89][0]
        last_message = json.loads(last_call_args[1].decode())
        assert abs(last_message["price"] - transformed_market_data["close_price"]) < 0.01

        assert mock_sleep.call_count == 90

    @pytest.mark.asyncio
    async def test_process_single_message_valid(self, producer, sample_market_data, transformed_market_data):
        """Test processing a single valid WebSocket message."""
        with mock.patch.object(producer, '_send_interpolated_price_ticks') as mock_send_interpolated:
            await producer._process_single_message(json.dumps(sample_market_data))

            producer.kafka_producer.send.assert_called_once()
            call_args = producer.kafka_producer.send.call_args[0]
            assert call_args[0] == "test_volume_stream"
            sent_data = json.loads(call_args[1].decode())
            assert sent_data == transformed_market_data

            mock_send_interpolated.assert_called_once_with(transformed_market_data)

    @pytest.mark.asyncio
    async def test_process_single_message_invalid(self, producer):
        """Test processing a single invalid WebSocket message."""
        invalid_data = {"d": "us_stocks_essential"}
        with mock.patch.object(producer, '_send_interpolated_price_ticks') as mock_send_interpolated, \
                mock.patch('logging.warning') as mock_logging:
            await producer._process_single_message(json.dumps(invalid_data))

            producer.kafka_producer.send.assert_not_called()
            mock_send_interpolated.assert_not_called()
            mock_logging.assert_called_once()

    @pytest.mark.asyncio
    async def test_handle_websocket_connection(self, producer, mock_websocket_connect, sample_market_data):
        """Test handling WebSocket connection and processing messages."""
        _, mock_websocket = mock_websocket_connect
        mock_websocket.__aiter__.return_value = [json.dumps(sample_market_data)]

        with mock.patch.object(producer, '_process_single_message') as mock_process:
            await producer._handle_websocket_connection()

            subscription_message = json.dumps({
                "event": "subscribe",
                "dataset": "us_stocks_essential",
                "tickers": ["AAPL", "MSFT"],
                "channel": "bars",
                "frequency": "1s",
                "aggregation": "1m"
            })
            mock_websocket.send.assert_called_once_with(subscription_message)

            mock_process.assert_called_once_with(json.dumps(sample_market_data))

    @staticmethod
    def test_run(producer):
        """Test the run method."""
        mock_process = mock.Mock()

        with mock.patch.object(producer, '_process_market_data', mock_process), \
                mock.patch('asyncio.run') as mock_run, \
                mock.patch.object(producer.kafka_producer, 'close') as mock_close:
            producer.run()

            assert mock_run.call_count == 1

            mock_close.assert_called_once()

    @staticmethod
    def test_run_with_keyboard_interrupt(producer):
        """Test the run method with keyboard interrupt."""
        mock_process = mock.Mock()

        def side_effect(*args, **kwargs):
            raise KeyboardInterrupt()

        with mock.patch('asyncio.run', side_effect=side_effect), \
                mock.patch.object(producer, '_process_market_data', mock_process), \
                mock.patch.object(producer.kafka_producer, 'close') as mock_close, \
                mock.patch('logging.info') as mock_logging:
            producer.run()

            mock_logging.assert_called_once_with("Application shutdown requested")

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
                    '--stream-topic', 'test_volume_stream',
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
