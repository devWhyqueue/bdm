from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging as log, os, requests, time, json

kafa_endpoint = os.environ['KAFKA_ENDPOINT']

producer = KafkaProducer(bootstrap_servers=[kafa_endpoint])
# future = producer.send('test-topic', b'Hello from producer!')

# query alpha vantage every 1 minute with config:
interval = 1
function = 'TIME_SERIES_INTRADAY'
equity = 'AAPL'

api_key_alpha_vantage = '' # insert your alpha vantage api key here
url_alpha_vantage = f'https://www.alphavantage.co/query?function={function}&symbol={equity}&interval={interval}min&apikey={api_key_alpha_vantage}'


while True:
    r = requests.get(url_alpha_vantage)
    data = r.json()
    print('Retrieved stock data:')
    print(data['Meta Data'])
    print(f'Values: {len(data["Time Series (1min)"])}')

    stock_ticks = list(data['Time Series (1min)'].values())
    for tick in stock_ticks:
        future = producer.send('test-topic', json.dumps(tick).encode())
        try:
            record_metadata = future.get(timeout=10)
        except KafkaError:
            log.exception()
            pass

        print('\nReceived Kafka Reply:')
        print(f'Topic: "{record_metadata.topic}", Partition: {record_metadata.partition}, Offset: {record_metadata.offset}')

        # make it a stream-like producer
        time.sleep(1)

    #wait 1min for the next call
    print('\nWait 1min for next request')
    time.sleep(60) # do we miss data here?

