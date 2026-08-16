import os
import time
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaError, KafkaException

load_dotenv()

KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'clickstream_events')
CONSUMER_GROUP = os.getenv('KAFKA_CONSUMER_GROUP', 'lakehouse-loader')
RAW_DATA_DIR = "data/raw"

# Batching Configuration
BATCH_SIZE_LIMIT = 500     
BATCH_TIME_LIMIT = 5.0     

# Build Aiven Kafka configuration using environment variables
consumer_config = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    'security.protocol': os.getenv('KAFKA_SECURITY_PROTOCOL', 'SASL_SSL'),
    'sasl.mechanism': os.getenv('KAFKA_SASL_MECHANISM', 'SCRAM-SHA-256'),
    'sasl.username': os.getenv('KAFKA_SASL_USERNAME'),
    'sasl.password': os.getenv('KAFKA_SASL_PASSWORD'),
    'group.id': CONSUMER_GROUP,
    'auto.offset.reset': 'earliest',  
    'enable.auto.commit': False      
}

consumer = Consumer(consumer_config)
consumer.subscribe([KAFKA_TOPIC])

def save_batch_to_parquet(events):
    """ Converts a list of event dicts to a compressed Parquet file. """
    if not events:
        return
    
    df = pd.DataFrame(events)
    table = pa.Table.from_pandas(df)
    
    timestamp_str = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S_%f")
    filename = f"clickstream_{timestamp_str}.parquet"
    filepath = os.path.join(RAW_DATA_DIR, filename)
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    
    pq.write_table(table, filepath, compression="snappy")
    print(f"Successfully saved {len(events)} events to: {filepath}")

def main():
    print(f"Consumer started. Listening to cloud topic '{KAFKA_TOPIC}'...")
    
    buffer = []
    last_flush_time = time.time()
    
    try:
        while True:
            # Poll Kafka with a 1.0-second timeout
            msg = consumer.poll(1.0)
            
            if msg is not None:
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event (safe to ignore)
                        continue
                    else:
                        raise KafkaException(msg.error())
                
                # Decode message payload
                payload = json.loads(msg.value().decode('utf-8'))
                buffer.append(payload)
            
            # Check if we should flush the buffer to a Parquet file
            current_time = time.time()
            elapsed_time = current_time - last_flush_time
            
            if len(buffer) >= BATCH_SIZE_LIMIT or (elapsed_time >= BATCH_TIME_LIMIT and len(buffer) > 0):
                save_batch_to_parquet(buffer)
                
                # Commit offsets to Kafka ONLY after data is safely written to disk
                consumer.commit(asynchronous=False)
                
                # Reset tracking variables
                buffer.clear()
                last_flush_time = time.time()
                
    except KeyboardInterrupt:
        print("\nStopping consumer process...")
    finally:
    
        if buffer:
            save_batch_to_parquet(buffer)
            consumer.commit(asynchronous=False)
        consumer.close()
        print("Consumer shut down cleanly.")

if __name__ == "__main__":
    main()