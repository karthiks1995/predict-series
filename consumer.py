from kafka import KafkaConsumer
import json
import io

topic = 'electric'
key_deserializer = 'org.apache.kafka.connect.storage.StringConverter'
value_deserializer = lambda m: json.loads(m.decode('ascii'))
group_id = 'electric-group'


consumer = KafkaConsumer(topic, group_id='consumer-grp', value_deserializer=value_deserializer)
print("Consumer connected : ", consumer.bootstrap_connected())
i = 0
for msg in consumer:
    print(i, " Message is : ",msg)
    i = i+1



