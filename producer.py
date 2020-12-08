from kafka import KafkaProducer
import msgpack

bootstrap_servers = 'localhost:9092'

value_serializer = msgpack.dumps

producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer = value_serializer)
print("Connection to kafka : ", producer.bootstrap_connected())
for _ in range(100):
    return_obj = producer.send('electric',value= "Haha", key=b'haha')