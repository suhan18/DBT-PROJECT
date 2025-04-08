from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "vehicle-data",
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id="test-consumer-group",
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Consumer started. Waiting for messages...\n")
for message in consumer:
    print(f"Received: {message.value}")
