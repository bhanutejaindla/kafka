from kafka import KafkaConsumer, KafkaProducer
import json
from datetime import datetime

# Kafka consumer (input)
consumer = KafkaConsumer(
    'raw-orders',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    group_id='transformer-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Kafka producer (output)
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("🔄 Listening on 'raw-orders' and transforming to 'enriched-orders'...\n")

for message in consumer:
    order = message.value

    # Add extra metadata (enrichment)
    enriched = {
        **order,
        "processed_at": datetime.now().isoformat(),
        "priority": "high" if order["price"] > 500 else "normal"
    }

    producer.send('enriched-orders', value=enriched)
    print(f"✨ Transformed & forwarded: {enriched}")
