import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "order-events",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    value_deserializer=lambda value: json.loads(value.decode("utf-8"))
)

print("Listening for order events...")

for message in consumer:
    print("Message received:", message.value)
