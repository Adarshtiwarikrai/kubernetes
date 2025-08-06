import json
import random
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer

# Local Docker Kafka (PLAINTEXT on port 9092)
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Function to generate order data
def generate_order(order_id):
    return {
        "order_id": order_id,
        "order_date": str((datetime.now() - timedelta(minutes=random.randint(0, 30))).isoformat()),
        "created_at": str(datetime.now().isoformat()),
        "customer_id": f"customer_{random.randint(1, 100)}",
        "amount": random.randint(100, 1000)
    }

# Publish orders with duplicates
try:
    order_id_counter = 1
    for _ in range(20):
        order_id = f"orders_topic_{order_id_counter}"
        order = generate_order(order_id)
        producer.send('orders_topic', value=order)
        print(f"Sent order: {order}")

        # Randomly duplicate the same order
        if random.choice([True, False]):
            producer.send('orders_topic', value=order)
            print(f"Sent duplicate order: {order}")

        order_id_counter += 1
        time.sleep(6)
finally:
    producer.flush()
    producer.close()
