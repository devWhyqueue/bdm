from kafka import KafkaConsumer
import json


print('Start consuming:')
consumer = KafkaConsumer('test-topic', 
                         bootstrap_servers='kafka:9092',
                         auto_offset_reset='earliest', 
                         enable_auto_commit=False,
                         #auto_commit_interval_ms=5000,
                         consumer_timeout_ms=10000, # timeout after 10s of no msgs
                         group_id='test_consumer_1',
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))

for message in consumer:
    # Process the message
    print(f"{message.topic}:{message.partition}:{message.offset}: key={message.key} value={message.value}")
    consumer.commit()
