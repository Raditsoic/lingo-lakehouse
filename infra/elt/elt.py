from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MinIO-Spark-ELT") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

input_path = "s3a://duolingo/raw/duolingo_batch_1362160679.parquet"
df = spark.read.parquet(input_path)

#transformed_df = df.withColumn("new_column", df["history_correct"] + df["session_correct"])
df.show()

ml_output_path = "s3a://duolingo-ml/duolingo_transformed.parquet"
visual_output_path = "s3a://duolingo-visual/duolingo_transformed.parquet"
# transformed_df.write.mode("overwrite").parquet(ml_output_path)

