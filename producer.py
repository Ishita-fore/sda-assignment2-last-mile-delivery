import csv
import json
import time
from kafka import KafkaProducer

KAFKA_SERVER = "localhost:9092"
TOPIC = "order-events"
CSV_FILE = "sample_order_data.csv"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

print("Starting Order Producer...")
print(f"Sending messages to Kafka topic: {TOPIC}")

with open(CSV_FILE, mode="r", newline="", encoding="utf-8") as file:
    reader = csv.DictReader(file)

    for row in reader:
        row["order_value"] = int(row["order_value"])

        producer.send(TOPIC, value=row)

        print(f"Message sent: {row}")

        time.sleep(2)

producer.flush()
producer.close()

print("All order messages sent successfully.")
