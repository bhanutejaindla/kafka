from kafka import KafkaProducer
import json, time, random
from datetime import datetime

# Create Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

users = ["alice", "bob", "charlie", "david"]
products = ["Laptop", "Keyboard", "Mouse", "Phone", "Monitor"]

print("🚀 Sending raw order data to Kafka topic 'raw-orders'...")

while True:
    order = {
        "order_id": random.randint(1000, 9999),
        "user": random.choice(users),
        "product": random.choice(products),
        "price": round(random.uniform(100, 1000), 2),
        "timestamp": datetime.now().isoformat()
    }

    producer.send('raw-orders', value=order)
    print(f"📦 Sent: {order}")
    time.sleep(2)  # wait 2 seconds before next message
