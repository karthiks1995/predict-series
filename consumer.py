from kafka import KafkaConsumer

import json

topic = ''
key_deserializer = 'org.apache.kafka.connect.storage.StringConverter'
value_deserializer = str
group_id = 'electric-group'

consumer = KafkaConsumer('electric', value_deserializer = value_deserializer)
print("Consumer connected : ", consumer.bootstrap_connected())
for msg in consumer:
    print("Message is : ",msg)

    # b'\x00\x00\x00\x00\x01\xd2\x9d\x11\x0cUser_3\x0ePage_67'
    # b'\\x00\\x00\\x00\\x00\\x01\\xb2\\xfb\\x11\\x0cUser_6\\x0ePage_13'