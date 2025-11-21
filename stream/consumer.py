# This Python code snippet is setting up a Kafka consumer using the `confluent_kafka` library to
# consume messages from a Kafka topic named 'posTransaction'.
# Note: This is testing script
from confluent_kafka import Consumer

consumerConfig = {
    'bootstrap.servers' : 'localhost:9092',
    'group.id' : 'transaction',
    'auto.offset.reset' : 'earliest'
}

consumer = Consumer(consumerConfig)
consumer.subscribe(['posTransaction'])

while True:
    msg = consumer.poll(1.0)
    if msg is None:
         continue
    elif msg.error():
        print(f"Error: {msg.error()}")
        continue
    else:
         print(f"Reciveing Message: {msg.value().decode('utf-8')}")

