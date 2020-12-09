import io

from confluent_kafka import Consumer, KafkaError
from avro.io import DatumReader, BinaryDecoder
import avro.schema

schema = avro.schema.parse(open("schema/PageViews.avsc", "rb").read())
reader = DatumReader(schema)
topic = 'electric'

def decode(msg_value):
    message_bytes = io.BytesIO(msg_value)
    decoder = BinaryDecoder(message_bytes)
    event_dict = reader.read(decoder)
    return event_dict

c = Consumer()
c.subscribe(topic)
running = True
while running:
    msg = c.poll()
    if not msg.error():
        msg_value = msg.value()
        event_dict = decode(msg_value)
        print(event_dict)
    elif msg.error().code() != KafkaError._PARTITION_EOF:
        print(msg.error())
        running = False