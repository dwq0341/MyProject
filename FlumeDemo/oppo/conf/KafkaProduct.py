# encoding: utf-8

from kafka import KafkaProducer
from kafka.errors import KafkaError
import json

#生产者
producer = KafkaProducer(
    bootstrap_servers=['172.16.216.236:9095'],
    value_serialize=lambda m: json.dumps(m).encode('ascii')
)

msg = "hello python".encode()
future = producer.send("mytopic1", msg)
try:
    record_metadata = future.get(timeout=10)
    print(record_metadata)
except KafkaError as e:
    print(e)
finally:
    producer.close()