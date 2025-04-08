from kafka import KafkaProducer
import threading
import time
import random
from datetime import datetime
import json

# Kafka configuration
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
topic = "vehicle-data"

# Data generation setup
locations = [
    "Indiranagar", "Whitefield", "Koramangala", "Jayanagar",
    "Marathahalli", "MG Road", "Hebbal", "BTM Layout",
    "Electronic City", "Rajajinagar", "Yeshwanthpur", "Banashankari"
]
vehicle_types = ["bike", "car", "truck", "auto", "bus"]
data_count = 0
lock = threading.Lock()

def generate_data():
    global data_count
    while True:
        location = random.choice(locations)
        vehicle = random.choice(vehicle_types)
        count = {
            "bike": random.randint(1, 11),
            "car": random.randint(1, 6),
            "truck": random.randint(1, 4),
            "auto": random.randint(1, 8),
            "bus": random.randint(1, 4)
        }[vehicle]

        data = {
            "timestamp": datetime.now().strftime("%H:%M:%S"),
            "location": location,
            "vehicle": vehicle,
            "count": count
        }

        # Send to Kafka
        producer.send(topic, value=data)
        print(f"Sent to Kafka: {data}")

        with lock:
            data_count += 1
        time.sleep(random.uniform(0.2, 0.5))

def print_data_count():
    global data_count
    while True:
        time.sleep(60)
        with lock:
            print(f"\n📊 Total data points generated so far: {data_count}\n")

# Start 4 data-generating threads
for _ in range(4):
    t = threading.Thread(target=generate_data)
    t.daemon = True
    t.start()

# Start counter display thread
count_thread = threading.Thread(target=print_data_count)
count_thread.daemon = True
count_thread.start()

# Keep main thread alive
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("Stopped.")
