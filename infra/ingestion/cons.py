from confluent_kafka import Consumer, KafkaError
from minio import Minio
import json
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta
import logging
import traceback

class KafkaConsumer:
    def __init__(self, kafka_config, minio_config, topic, batch_window=5):
        """
        Initialize Kafka Consumer with MinIO uploader
        
        :param kafka_config: Configuration for Kafka consumer
        :param minio_config: Configuration for MinIO client
        :param topic: Kafka topic to consume
        :param batch_window: Time window for batching messages (in minutes)
        """

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s: %(message)s'
        )
        self.logger = logging.getLogger(__name__)

        self.consumer_config = {
            **kafka_config,
            'enable.auto.commit': False,  # Manual offset management
            'max.poll.interval.ms': 300000,  # 5 minute max poll interval
            'session.timeout.ms': 45000  
        }
        self.consumer = Consumer(self.consumer_config)
        self.topic = topic

        self.minio_client = Minio(
            minio_config['endpoint'],
            access_key=minio_config['access_key'],
            secret_key=minio_config['secret_key'],
            secure=minio_config.get('secure', False)
        )
        self.bucket_name = minio_config['bucket_name']

        self.batch_window = timedelta(minutes=batch_window)
        self.data_list = []
        self.window_start = datetime.now()

        self._create_bucket_if_not_exists()

    def _create_bucket_if_not_exists(self):
        try:
            if not self.minio_client.bucket_exists(self.bucket_name):
                self.minio_client.make_bucket(self.bucket_name)
                self.logger.info(f"Created bucket {self.bucket_name}")
        except Exception as e:
            self.logger.error(f"Error creating bucket: {e}")
            raise

    def _upload_to_minio(self, df, timestamp=None):
        """
        Upload DataFrame to MinIO as Parquet
        
        :param df: DataFrame to upload
        :param timestamp: Optional timestamp for filename
        """
        try:
            parquet_buffer = BytesIO()
            df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
            parquet_buffer.seek(0)

            # Generate unique object name
            timestamp_str = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
            object_name = f"raw/duolingo_batch_{timestamp_str}.parquet"

            self.minio_client.put_object(
                bucket_name=self.bucket_name,
                object_name=object_name,
                data=parquet_buffer,
                length=parquet_buffer.getbuffer().nbytes,
                content_type="application/octet-stream"
            )
            self.logger.info(f"Uploaded {object_name} to MinIO")
        except Exception as e:
            self.logger.error(f"Error uploading to MinIO: {e}")

    def consume(self):
        """
        Main consumption loop with robust error handling
        """
        try:
            self.consumer.subscribe([self.topic])
            self.logger.info(f"Consuming messages from topic: {self.topic}")

            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    self._check_and_process_batch()
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        self.logger.info("Reached end of partition")
                        continue
                    else:
                        self.logger.error(f"Consumer error: {msg.error()}")
                        break

                try:
                    record = json.loads(msg.value().decode('utf-8'))
                    self.data_list.append(record)

                    self.consumer.commit(msg)
                except json.JSONDecodeError:
                    self.logger.error("Failed to decode message")
                    continue
                except Exception as e:
                    self.logger.error(f"Error processing message: {e}")
                    continue

                self._check_and_process_batch()

        except KeyboardInterrupt:
            self.logger.info("Shutting down consumer...")
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
            traceback.print_exc()
        finally:
            # Process any remaining data
            self._final_batch_upload()
            self.consumer.close()

    def _check_and_process_batch(self):
        """
        Check if batch window is complete and process data
        """
        current_time = datetime.now()
        if current_time - self.window_start >= self.batch_window and self.data_list:
            try:
                df = pd.DataFrame(self.data_list)
                self._upload_to_minio(df)
                
                # Reset batch
                self.data_list = []
                self.window_start = current_time
            except Exception as e:
                self.logger.error(f"Error processing batch: {e}")

    def _final_batch_upload(self):
        """
        Upload any remaining data when consumer is closing
        """
        if self.data_list:
            try:
                df = pd.DataFrame(self.data_list)
                self._upload_to_minio(df, "final")
            except Exception as e:
                self.logger.error(f"Error in final batch upload: {e}")

def main():
    # Kafka configuration
    kafka_conf = {
        'bootstrap.servers': 'localhost:29092',
        'group.id': 'duolingo-group',
        'auto.offset.reset': 'earliest'
    }

    # MinIO configuration
    minio_conf = {
        'endpoint': 'localhost:9000',
        'access_key': 'minio',
        'secret_key': 'minio123',
        'secure': False,
        'bucket_name': 'duolingo'
    }

    consumer = KafkaConsumer(
        kafka_config=kafka_conf, 
        minio_config=minio_conf, 
        topic="duolingo",
        batch_window=5  # 5-minute batch 
    )
    consumer.consume()

if __name__ == "__main__":
    main()