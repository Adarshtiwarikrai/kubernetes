import json
import random
import uuid
from datetime import datetime, timedelta
from kafka import KafkaProducer

# Initialize Kafka producer for local Docker Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # or 'broker:29092' if in Docker network
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Function to generate payment data
def generate_payment(order_id, payment_id):
    return {
        "payment_id": payment_id,
        "order_id": order_id,
        "payment_date": str((datetime.now() - timedelta(minutes=random.randint(0, 30))).isoformat()),
        "created_at": str(datetime.now().isoformat()),
        "amount": random.randint(50, 500)
    }

# Specify order_id and publish a single payment
order_id = "order_2"
payment_id = str(uuid.uuid4())

try:
    payment = generate_payment(order_id, payment_id)
    producer.send('payments-topic', value=payment)
    print(f"Sent payment: {payment}")
finally:
    producer.flush()
    producer.close()
