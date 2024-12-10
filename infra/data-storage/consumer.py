from confluent_kafka import Consumer
from minio import Minio
import json
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta

conf = {
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'duolingo-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
topic = "duolingo"
consumer.subscribe([topic])

minio_client = Minio(
    "localhost:9000",  
    access_key="minio",
    secret_key="minio123",
    secure=False  
)

bucket_name = "duolingo"
if not minio_client.bucket_exists(bucket_name):
    minio_client.make_bucket(bucket_name)

print("Listening to Kafka topic and uploading to MinIO...")

data_list = []
window_start = datetime.now()

try:
    while True:
        msg = consumer.poll(1.0) 
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        record = json.loads(msg.value().decode('utf-8'))
        data_list.append(record)

        print(data_list)

        current_time = datetime.now()
        if current_time - window_start >= timedelta(minutes=1):
            df = pd.DataFrame(data_list)
            print(df)
            parquet_buffer = BytesIO()
            df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
            parquet_buffer.seek(0)

            object_name = f"raw/duolingo_batch_{record['timestamp']}.parquet"
            minio_client.put_object(
                bucket_name=bucket_name,
                object_name=object_name,
                data=parquet_buffer,
                length=parquet_buffer.getbuffer().nbytes,
                content_type="application/octet-stream"
            )
            print(f"Uploaded {object_name} to MinIO")

            data_list = []
            window_start = current_time

except KeyboardInterrupt:
    print("Shutting down consumer...")

finally:
    consumer.close()

    if data_list:
        df = pd.DataFrame(data_list)
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
        parquet_buffer.seek(0)

        object_name = f"raw/duolingo_batch_final.parquet"
        minio_client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=parquet_buffer,
            length=parquet_buffer.getbuffer().nbytes,
            content_type="application/octet-stream"
        )
        print(f"Uploaded {object_name} to MinIO")

