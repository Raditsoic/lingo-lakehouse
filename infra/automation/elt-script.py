from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from minio import Minio

spark = SparkSession.builder \
    .appName("MinIO-Spark-ELT") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3n.impl", "org.apache.hadoop.fs.s3native.S3NativeFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

minio_client = Minio(
    "localhost:9000",
    access_key="minio",
    secret_key="minio123",
    secure=False
)

bucket_name = "duolingo"

objects = minio_client.list_objects(bucket_name, prefix="raw/", recursive=True)

latest_file = None
latest_timestamp = 0

for obj in objects:
    # Extract timestamp from the object name (assuming naming convention duolingo_batch_<timestamp>.parquet)
    try:
        timestamp = int(obj.object_name.split('_')[-1].split('.')[0])  # Example: 1362160679
        if timestamp > latest_timestamp:
            latest_timestamp = timestamp
            latest_file = obj.object_name
    except ValueError:
        continue

if latest_file:
    input_path = f"s3a://{bucket_name}/{latest_file}"
    print(f"Reading latest batch file: {latest_file}")
else:
    print("No batch files found.")
    input_path = None

if input_path:
    df = spark.read.parquet(input_path)

    # Machine Learning Data
    ml_df = df.drop('ui_language', 'lexeme_string', 'timestamp')
    ml_df =  ml_df.withColumn('accuracy_rate', col('history_correct') / col('history_seen')) \
            .withColumn('session_accuracy', col('session_correct') / col('session_seen')) \
            .withColumn('delta_days', col('delta') / (60 * 60 * 24))

    ml_df = ml_df.drop('delta', 'history_seen', 'history_correct', 'session_seen', 'session_correct')

    # Application Database
    warehouse_df = df.drop('ui_language')

    # Metabase/Visualization Dataframe
    metabase_df = df.drop('ui_language', 'lexeme_id', 'timestamp', 'learning_language', 'delta', 'user_id')

    metabase_df = metabase_df.withColumn(
        "error_rate", 
        1 - (F.col("session_correct") / F.col("session_seen"))
    )

    metabase_df = metabase_df.groupBy("lexeme_string").agg(
        F.mean("p_recall").alias("avg_recall"),
        F.mean("error_rate").alias("error_rate"),
        F.sum("session_seen").alias("total_seen")
    )

    ml_output_path = f"s3a://{bucket_name}/ml/duolingo_transformed_{latest_timestamp}.csv"
    visual_output_path = f"s3a://{bucket_name}/visual/duolingo_transformed_{latest_timestamp}.csv"
    warehouse_output_path = f"s3a://{bucket_name}/database/duolingo_transformed_{latest_timestamp}.csv"

    ml_df.write.mode("overwrite").option("header", "true").csv(ml_output_path)
    metabase_df.write.mode("overwrite").option("header", "true").csv(visual_output_path)
    warehouse_df.write.mode("overwrite").option("header", "true").csv(warehouse_output_path)
else:
    print("No data to process.")