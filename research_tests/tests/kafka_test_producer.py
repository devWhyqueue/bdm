from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging as log, os

kafa_endpoint = os.environ['KAFKA_ENDPOINT']

producer = KafkaProducer(bootstrap_servers=[kafa_endpoint])
future = producer.send('test-topic', b'Hello from producer!')

# Block for 'synchronous' sends
try:
    record_metadata = future.get(timeout=10)
except KafkaError:
    log.exception()
    pass

# Successful result returns assigned partition and offset
print(record_metadata.topic)
print(record_metadata.partition)
print(record_metadata.offset)