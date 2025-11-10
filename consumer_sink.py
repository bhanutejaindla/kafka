from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'enriched-orders',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    group_id='sink-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("💾 Consuming 'enriched-orders' and saving to file...\n")

output_file = "final_orders.json"

for message in consumer:
    enriched_order = message.value
    print(f"✅ Received enriched order: {enriched_order}")

    # Append to file
    with open(output_file, "a") as f:
        f.write(json.dumps(enriched_order) + "\n")
