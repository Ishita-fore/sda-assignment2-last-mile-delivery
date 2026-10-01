import json
import mysql.connector
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "order-events",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    value_deserializer=lambda value: json.loads(value.decode("utf-8"))
)

db = mysql.connector.connect(
    host="localhost",
    port=3306,
    user="root",
    password="&Password271104"
)

cursor = db.cursor()

cursor.execute("CREATE DATABASE IF NOT EXISTS stockdb")
cursor.execute("USE stockdb")

cursor.execute("""
CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(20),
    store_id VARCHAR(20),
    customer_zone VARCHAR(20),
    order_status VARCHAR(30),
    order_value INT,
    timestamp DATETIME
)
""")

db.commit()

print("Consumer started...")
print("Waiting for order events...")

for message in consumer:
    data = message.value

    cursor.execute("""
        INSERT INTO orders
        (order_id, store_id, customer_zone, order_status, order_value, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        data["order_id"],
        data["store_id"],
        data["customer_zone"],
        data["order_status"],
        data["order_value"],
        data["timestamp"]
    ))

    db.commit()

    print("Stored in MySQL:", data)
