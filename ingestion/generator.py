import os
import json
import random
import time
from datetime import datetime
from dotenv import load_dotenv
from confluent_kafka import Producer

load_dotenv()

producer_config = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    'security.protocol': os.getenv('KAFKA_SECURITY_PROTOCOL', 'SASL_SSL'),
    'sasl.mechanism': os.getenv('KAFKA_SASL_MECHANISM', 'SCRAM-SHA-256'),
    'sasl.username': os.getenv('KAFKA_SASL_USERNAME'),
    'sasl.password': os.getenv('KAFKA_SASL_PASSWORD'),
    'client.id': 'clickstream-generator'
}

producer = Producer(producer_config)

KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'clickstream_events')

# Mock Data Templates
USER_IDS = [f"USR_{i:04d}" for i in range(1, 101)]
PRODUCT_IDS = [f"PROD_{i:03d}" for i in range(1, 21)]
EVENT_TYPES = ["view", "view", "view", "cart_add", "cart_add", "purchase"]
DEVICES = ["desktop", "mobile", "tablet"]

def delivery_report(err, msg):
    """ Callback to confirm delivery of message to Kafka. """
    if err is not None:
        print(f"Delivery failed for Message: {err}")
    else:
        print(f"Message delivered to topic '{msg.topic()}' partition [{msg.partition()}] at offset {msg.offset()}")

def generate_clickstream_event():
    """ Simulates a live e-commerce user action. """
    return {
        "event_id": f"evt_{random.getrandbits(32)}",
        "user_id": random.choice(USER_IDS),
        "product_id": random.choice(PRODUCT_IDS),
        "event_type": random.choice(EVENT_TYPES),
        "device": random.choice(DEVICES),
        "timestamp": datetime.utcnow().isoformat()
    }

if __name__ == "__main__":
    print(f"Starting live mock streaming to Aiven Cloud Kafka on topic '{KAFKA_TOPIC}'...")
    
    try:
        while True:
            payload = generate_clickstream_event()     
            message_bytes = json.dumps(payload).encode('utf-8')
    
            producer.produce(
                topic=KAFKA_TOPIC,
                value=message_bytes,
                key=payload["user_id"].encode('utf-8'),
                callback=delivery_report
            )
            
            producer.poll(0)
            
            time.sleep(random.uniform(0.1, 0.8))
            
    except KeyboardInterrupt:
        print("\nStopping data generator...")
    finally:
        print("Flushing final producer messages...")
        producer.flush()
