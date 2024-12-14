import os
from pyspark.sql import SparkSession
from minio import Minio
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create a configured Spark session."""
    try:
        spark = SparkSession.builder \
            .appName("MinIO-Spark-ELT") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0,org.apache.hadoop:hadoop-aws:3.2.0") \
            .config("spark.hadoop.fs.s3a.access.key", os.getenv('MINIO_ACCESS_KEY', 'minio')) \
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv('MINIO_SECRET_KEY', 'minio123')) \
            .config("spark.hadoop.fs.s3a.endpoint", os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3n.impl", "org.apache.hadoop.fs.s3native.S3NativeFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .getOrCreate()
        return spark
    except Exception as e:
        logger.error(f"Failed to create Spark session: {e}")
        raise

def get_latest_file(minio_client, bucket_name, prefix):
    """Find the latest file in a given bucket and prefix."""
    try:
        files = minio_client.list_objects(bucket_name, prefix=prefix, recursive=True)
        latest_file = None
        latest_timestamp = 0

        for file in files:
            try:
                # More robust timestamp extraction
                timestamp = int(file.object_name.split('_')[-1].split('.')[0])
                if timestamp > latest_timestamp:
                    latest_timestamp = timestamp
                    latest_file = file.object_name
            except (ValueError, IndexError):
                logger.warning(f"Skipping file with non-standard name: {file.object_name}")

        return f"s3a://{bucket_name}/{latest_file}" if latest_file else None
    except Exception as e:
        logger.error(f"Error finding latest file in {prefix}: {e}")
        return None

def read_csv_with_schema(spark, input_path):
    """Read CSV with more robust options."""
    try:
        df = spark.read.csv(
            input_path, 
            header=True,  
            inferSchema=True,  
            ignoreLeadingWhiteSpace=True,
            ignoreTrailingWhiteSpace=True
        )
        
        logger.info(f"Schema for {input_path}: {df.schema}")
        return df
    except Exception as e:
        logger.error(f"Failed to read CSV from {input_path}: {e}")
        raise

def transfer_data(spark, input_path, target_db, target_table, postgres_properties):
    """Transfer data from input path to PostgreSQL."""
    try:
        df = read_csv_with_schema(spark, input_path)
        print("File:", input_path)
        print(df.printSchema())
        print(df.show())

        df.write.jdbc(
            url=f"jdbc:postgresql://localhost:5432/{target_db}",
            table=target_table,
            mode="append", 
            properties=postgres_properties
        )
        logger.info(f"Data written to {target_db}.{target_table}")
    except Exception as e:
        logger.error(f"Data transfer failed for {target_table}: {e}")
        raise

def main():
    spark = create_spark_session()
    
    minio_client = Minio(
        os.getenv('MINIO_ENDPOINT', 'localhost:9000').replace('http://', ''),
        access_key=os.getenv('MINIO_ACCESS_KEY', 'minio'),
        secret_key=os.getenv('MINIO_SECRET_KEY', 'minio123'),
        secure=False
    )

    bucket_name = "duolingo"
    
    postgres_properties = {
        "user": os.getenv('POSTGRES_USER', 'soic'),
        "password": os.getenv('POSTGRES_PASSWORD', '123'),
        "driver": "org.postgresql.Driver"
    }

    data_configs = [
        {"prefix": "ml/", "db": "mldb", "table": "ml_data"},
        {"prefix": "visual/", "db": "metabase", "table": "visualization_data"},
        {"prefix": "database/", "db": "appdb", "table": "app_data"}
    ]

    for config in data_configs:
        try:
            input_path = get_latest_file(minio_client, bucket_name, config['prefix'])
            
            if input_path:
                transfer_data(
                    spark, 
                    input_path, 
                    config['db'], 
                    config['table'], 
                    postgres_properties
                )
            else:
                logger.warning(f"No files found in {config['prefix']}")
        except Exception as e:
            logger.error(f"Processing failed for {config['prefix']}: {e}")

    logger.info("Data transfer complete.")

if __name__ == "__main__":
    main()