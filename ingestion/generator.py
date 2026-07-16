import json
import random
import time
from datetime import datetime
from confluent_kafka import Producer

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "live_clicks"

# Initialize Kafka Producer
producer_config = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'client.id': 'clickstream-generator'
}
producer = Producer(producer_config)

# Mock Data Templates
USER_IDS = [f"USR_{i:04d}" for i in range(1, 101)]
PRODUCT_IDS = [f"PROD_{i:03d}" for i in range(1, 21)]
EVENT_TYPES = ["view", "view", "view", "cart_add", "cart_add", "purchase"]  # Weighting views higher
DEVICES = ["desktop", "mobile", "tablet"]

def delivery_report(err, msg):
    """ Callback to confirm delivery of message to Kafka. """
    if err is not None:
        print(f"Delivery failed for Message: {err}")
    else:
        print(f"Message delivered to partition [{msg.partition()}] at offset {msg.offset()}")

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
    print(f"Starting live mock streaming to Kafka on topic '{KAFKA_TOPIC}'...")
    
    try:
        while True:
            # Generate clickstream payload
            payload = generate_clickstream_event()
            
            # Serialize JSON payload
            message_bytes = json.dumps(payload).encode('utf-8')
            
            # Send asynchronously to Kafka broker
            producer.produce(
                topic=KAFKA_TOPIC,
                value=message_bytes,
                key=payload["user_id"].encode('utf-8'), # Key helps with partition consistency
                callback=delivery_report
            )
            
            # Flush producer queue
            producer.poll(0)
            
            # Sleep randomly to simulate dynamic user traffic
            time.sleep(random.uniform(0.1, 0.8))
            
    except KeyboardInterrupt:
        print("\nStopping data generator...")
    finally:
        print("Flushing final producer messages...")
        producer.flush()

        