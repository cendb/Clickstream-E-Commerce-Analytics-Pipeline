import os
import time
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaError, KafkaException

# Configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "live_clicks"
CONSUMER_GROUP = "lakehouse-loader"
RAW_DATA_DIR = "data/raw"

# Batching Configuration
BATCH_SIZE_LIMIT = 500       # Maximum number of messages per Parquet file
BATCH_TIME_LIMIT = 5.0       # Maximum seconds to wait before writing a file

# Initialize Kafka Consumer
consumer_config = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': CONSUMER_GROUP,
    'auto.offset.reset': 'earliest',  # Start reading from the beginning if no offset exists
    'enable.auto.commit': False       # We will commit manually after writing the Parquet file (At-Least-Once guarantee)
}
consumer = Consumer(consumer_config)
consumer.subscribe([KAFKA_TOPIC])

def save_batch_to_parquet(events):
    """ Converts a list of event dicts to a compressed Parquet file. """
    if not events:
        return
    
    # 1. Convert list of JSON objects to a Pandas DataFrame
    df = pd.DataFrame(events)
    
    # 2. Convert DataFrame to PyArrow Table
    table = pa.Table.from_pandas(df)
    
    # 3. Generate a unique filename using a microsecond timestamp
    timestamp_str = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S_%f")
    filename = f"clickstream_{timestamp_str}.parquet"
    filepath = os.path.join(RAW_DATA_DIR, filename)
    
    # Ensure raw directory exists
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    
    # 4. Write optimized, Snappy-compressed Parquet file
    pq.write_table(table, filepath, compression="snappy")
    print(f"Successfully saved {len(events)} events to: {filepath}")

def main():
    print(f"Consumer started. Listening to '{KAFKA_TOPIC}'...")
    
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
        # Save any remaining events in the buffer before exiting
        if buffer:
            save_batch_to_parquet(buffer)
            consumer.commit(asynchronous=False)
        consumer.close()
        print("Consumer shut down cleanly.")

if __name__ == "__main__":
    main()