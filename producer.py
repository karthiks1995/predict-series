from kafka import KafkaProducer
import msgpack
import time
import json

bootstrap_servers = 'localhost:9092'

value_serializer = msgpack.dumps
topic = 'electric'

producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
print("Connection to kafka : ", producer.bootstrap_connected())
for i in range(100):
    time.sleep(1)
    # return_obj = producer.send('electric', b'some_message_bytes')
    producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii'))
    producer.send(topic, {'key': i * 2})
    # producer.send('electric', {'key': 'value'})