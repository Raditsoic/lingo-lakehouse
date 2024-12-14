from confluent_kafka import Producer
import pandas as pd
import json
import time
import random
import sys

class KafkaProducer:
    def __init__(self, bootstrap_servers, topic, chunk_size=10000):
        """
        Initialize Kafka Producer with memory-efficient streaming
        
        :param bootstrap_servers: Kafka broker address
        :param topic: Kafka topic to produce messages
        :param chunk_size: Number of rows to process in each batch
        """
        self.conf = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'efficient-producer',
            'batch.size': 16384,  # 16 KB batch size
            'linger.ms': 5,  # Small delay to improve batching
            'compression.type': 'snappy',  # Efficient compression
            'queue.buffering.max.messages': 100000,
            'queue.buffering.max.kbytes': 1048576,  # 1 GB buffer
        }
        self.producer = Producer(self.conf)
        self.topic = topic
        self.chunk_size = chunk_size

    def delivery_report(self, err, msg):
        """
        Callback for message delivery reports
        """
        if err is not None:
            print(f'Message delivery failed: {err}')
        else:
            pass

    def stream_csv(self, csv_path, simulation_mode=True, min_delay=0.1, max_delay=1.0):
        """
        Stream CSV file to Kafka with memory-efficient approach
        
        :param csv_path: Path to the CSV file
        :param streaming_mode: Simulate real-time streaming
        :param min_delay: Minimum delay between message batches
        :param max_delay: Maximum delay between message batches
        """
    
        csv_iterator = pd.read_csv(csv_path, chunksize=self.chunk_size)
        
        try:
            for chunk_index, chunk in enumerate(csv_iterator):
                print(f"Processing chunk {chunk_index + 1}")
                
                messages = self.process_chunk(chunk)
                
                for message in messages:
                    self.producer.produce(
                        self.topic, 
                        key=message['key'], 
                        value=json.dumps(message['value']), 
                        callback=self.delivery_report
                    )
                
                # Flush prevent buffer overflow
                self.producer.poll(0)
                
                if simulation_mode:
                    time.sleep(random.uniform(min_delay, max_delay))
        
        except Exception as e:
            print(f"Error processing CSV: {e}")
            sys.exit(1)
        
        finally:
            self.producer.flush()
            print("All messages sent successfully.")

    def process_chunk(self, chunk):
        """
        Convert DataFrame chunk to list of messages
        
        :param chunk: DataFrame chunk
        :return: List of messages
        """
        messages = []
        for index, row in chunk.iterrows():
            message = {
                'key': str(index),
                'value': {
                    "user_id": row["user_id"],
                    "timestamp": row["timestamp"],
                    "learning_language": row["learning_language"],
                    "ui_language": row["ui_language"],
                    "lexeme_id": row["lexeme_id"],
                    "lexeme_string": row["lexeme_string"],
                    "delta": row["delta"],
                    "p_recall": row["p_recall"],
                    "history_seen": row["history_seen"],
                    "history_correct": row["history_correct"],
                    "session_seen": row["session_seen"],
                    "session_correct": row["session_correct"],
                }
            }
            messages.append(message)
        return messages

# Usage
if __name__ == "__main__":
    producer = KafkaProducer(
        bootstrap_servers='localhost:29092', 
        topic='duolingo'
    )
    producer.stream_csv('dataset/duolingo-spaced-repetition-data.csv')