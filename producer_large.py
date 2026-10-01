import json
import random
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer

KAFKA_SERVER = "localhost:9092"
TOPIC = "order-events"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

stores = ["DS101", "DS102", "DS103", "DS104", "DS105"]
zones = ["Z01", "Z02", "Z03", "Z04", "Z05", "Z06", "Z07", "Z08"]

statuses = [
    "ORDER_PLACED",
    "ORDER_ACCEPTED",
    "READY_FOR_PICKUP",
    "PICKED_UP",
    "DELIVERED"
]

start_time = datetime(2026, 9, 30, 14, 0, 0)

print("Starting Large Order Producer...")
print(f"Sending messages to Kafka topic: {TOPIC}")

for i in range(1, 201):

    order = {
        "order_id": f"ORD{2000 + i}",
        "store_id": random.choice(stores),
        "customer_zone": random.choice(zones),
        "order_status": random.choice(statuses),
        "order_value": random.randint(300, 3000),
        "timestamp": (
            start_time + timedelta(seconds=i * 10)
        ).strftime("%Y-%m-%d %H:%M:%S")
    }

    producer.send(TOPIC, value=order)

    print(f"Message sent: {order}")

    time.sleep(1)

producer.flush()
producer.close()

print("All 200 order messages sent successfully.")
