from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging as log, os, time, json, rel, websocket

API_KEY = os.environ['API_KEY']
TICKERS = [os.environ['TICKER']]
FREQUENCY = os.environ['FREQUENCY']
DATASET = 'us_stocks_essential'
KAFKA_ENDPOINT = os.environ['KAFKA_ENDPOINT']

def on_error(wsapp, error):
    print(f'Error: {error}')


def on_close(wsapp, close_status_code, close_msg):
    print('Connection is closed')


def on_open(wsapp):
    print('Connection is opened')
    subscribe(wsapp, DATASET, TICKERS, FREQUENCY)


def subscribe(wsapp, dataset, tickers, frequency):
    sub_request = {
        'event': 'subscribe',
        'dataset': dataset,
        'tickers': tickers,
        'channel': 'bars',
        'frequency': frequency,
        'aggregation': '1m'
    }
    wsapp.send(json.dumps(sub_request))


if __name__ == '__main__':
    # open a connection to our kafka service
    producer = KafkaProducer(bootstrap_servers=[KAFKA_ENDPOINT])

    def on_message(wsapp, message):
        stock_data = json.loads(message)
        if not ('o' in stock_data and 'c' in stock_data):
            print(f'Received message: {message}')
            return

        print(f'Received Stock Data: {stock_data}')

        # artificially extend data by linear interpolation (10 ticks/s)
        # since the websocket connection often takes>1s we prolong each fetch to 3s to have a proper stream
        num_points_s = 30
        kafka_replies = ['Received Kafka Replies:']
        for i in range(num_points_s):
            # linear interpolation between open price and close price
            kafka_data = { "p": round(stock_data['o'] + i * ((stock_data['c'] - stock_data['o']) / (num_points_s + 1)), 2) }
            future = producer.send('test-topic', json.dumps(kafka_data).encode())
            print(f'Sent: {kafka_data}')
            try:
                record_metadata = future.get(timeout=10)
            except KafkaError:
                log.exception()
                pass

            kafka_replies.append(f'{record_metadata.topic}:{record_metadata.partition}:{record_metadata.offset}')
            time.sleep(0.1)
        print(" ".join(kafka_replies))
    
    # Open ws connection
    ws = websocket.WebSocketApp(f'wss://ws.finazon.io/v1?apikey={API_KEY}',
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error)
    # Start event loop
    ws.run_forever(
        # Set dispatcher to automatic reconnection, 5 second reconnect delay if connection closed unexpectedly
        dispatcher=rel, reconnect=5,
        # Sending ping with specified interval to prevent disconnecting
        ping_interval=60, ping_timeout=20,
    )
    # Handle Keyboard Interrupt event
    rel.signal(2, rel.abort)
    rel.dispatch()
