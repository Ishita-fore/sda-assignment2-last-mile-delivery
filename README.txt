*# sda-assignment2-last-mile-delivery*

Real-Time Last-Mile Delivery Analytics - Assignment 2

Industry: Logistics & Supply Chain
Sub-Industry: Last-Mile Delivery for Quick Commerce & E-Commerce

Objective:
Create sample order data and stream it to Apache Kafka using a Python producer.

Flow:
Order Management System (Sample CSV) -> producer.py -> Apache Kafka -> order-events -> consumer.py -> Terminal

Kafka Topic:
order-events

Sample Data:
The sample dataset is stored in data/sample_order_data.csv.

Format: CSV
Records: 15
Fields:
- order_id
- store_id
- customer_zone
- order_status
- order_value
- timestamp

The dataset was manually created based on the Order Management System identified in Assignment 1. It contains different order stages such as order placed, order accepted, ready for pickup, picked up and delivered.

Producer:
producer.py reads the CSV file row by row, converts each row into JSON format and sends it to the order-events Kafka topic. A 2-second delay is used between messages to simulate real-time streaming.

Consumer:
consumer.py subscribes to the order-events topic and displays the messages received from Kafka in the terminal.

Requirements:
- Python 3
- Docker Desktop
- Apache Kafka
- kafka-python

Run:
1. Start Kafka using Docker.
2. Run consumer.py:
   python consumer.py
3. In another terminal, run:
   python producer.py
