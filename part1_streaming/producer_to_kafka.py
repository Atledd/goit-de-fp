import json
import time

import mysql.connector
from kafka import KafkaProducer


connection = mysql.connector.connect(
    host="localhost",
    port=3307,
    user="root",
    password="root",
    database="olympic_dataset"
)

cursor = connection.cursor(dictionary=True)

cursor.execute("""
    SELECT athlete_id, sport, medal
    FROM athlete_event_results
""")

rows = cursor.fetchall()


producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


for row in rows:
    producer.send("athlete_event_results", row)
    print("Sent:", row)
    time.sleep(2)

producer.flush()

cursor.close()
connection.close()

print("All events sent to Kafka")