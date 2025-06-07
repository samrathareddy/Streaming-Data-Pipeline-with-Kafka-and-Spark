from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, lit
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
import logging
import great_expectations as ge
import psycopg2
import os

# Logging Configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Kafka Configuration
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "sensor_readings"
CONSUMER_GROUP = "sensor_readings_consumer"

# MinIO (S3-Compatible Storage) Configuration
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
RAW_DATA_PATH = "s3a://raw-data/streaming_raw/"
ANOMALY_DATA_PATH = "s3a://processed-data/streaming_anomalies/"

# PostgreSQL Configuration
POSTGRES_HOST = "postgres"
POSTGRES_PORT = "5432"
POSTGRES_DB = "processed_db"
POSTGRES_USER = "user"
POSTGRES_PASSWORD = "pass"

# Define schema for incoming JSON
schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("timestamp", LongType(), False),
    StructField("device_id", LongType(), False),
    StructField("reading_value", DoubleType(), False)
])


def validate_schema(df):
    """
    Validate the schema of the incoming streaming DataFrame using Great Expectations.
    Ensures:
      - 'event_id' is non-null and unique
      - 'timestamp' is non-null and positive
      - 'device_id' is non-null and positive
      - 'reading_value' is non-null and within expected ranges (0-100)
    """
    logging.info("Validating streaming data schema with Great Expectations...")

    ge_df = ge.from_pandas(df.toPandas())  # Convert Spark DataFrame to Pandas for validation

    # Expect event_id to be unique and non-null
    result_event_id = ge_df.expect_column_values_to_not_be_null("event_id")
    if not result_event_id.success:
        raise ValueError("Validation failed: 'event_id' contains null values")

    # Expect timestamp to be non-null and positive
    result_timestamp = ge_df.expect_column_values_to_be_between("timestamp", min_value=1)
    if not result_timestamp.success:
        raise ValueError("Validation failed: 'timestamp' contains negative or null values")

    # Expect device_id to be non-null and positive
    result_device_id = ge_df.expect_column_values_to_be_between("device_id", min_value=1)
    if not result_device_id.success:
        raise ValueError("Validation failed: 'device_id' contains negative values")

    # Expect reading_value to be between 0 and 100 (sensor range)
    result_reading_value = ge_df.expect_column_values_to_be_between("reading_value", min_value=0, max_value=100)
    if not result_reading_value.success:
        raise ValueError("Validation failed: 'reading_value' is outside the expected range")

    logging.info("Schema validation passed.")


def save_to_postgres(df, table_name):
    """
    Saves the processed DataFrame to PostgreSQL.
    """
    logging.info(f"Writing data to PostgreSQL table: {table_name}...")

    try:
        df.write \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}") \
            .option("dbtable", table_name) \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

        logging.info(f"Successfully written to PostgreSQL table: {table_name}")

    except Exception as e:
        logging.error(f"Failed to write to PostgreSQL: {str(e)}")


def main():
    """
    Streaming ETL Pipeline:
    - Reads real-time sensor data from Kafka
    - Parses JSON messages into structured Spark DataFrame
    - Validates schema using Great Expectations
    - Filters out malformed and invalid records
    - Detects anomalies (reading_value > 70)
    - Writes anomalies to PostgreSQL & MinIO for analytics
    - Writes raw and cleaned data for historical tracking
    """
    try:
        # Initialize Spark session
        spark = SparkSession.builder \
            .appName("StreamingETL") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.2.18") \
            .getOrCreate()

        # Configure Spark to access MinIO using s3a
        spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", MINIO_ENDPOINT)
        spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", MINIO_ACCESS_KEY)
        spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", MINIO_SECRET_KEY)
        spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")

        logging.info(f"Starting Spark Structured Streaming from Kafka topic: {KAFKA_TOPIC}")

        # Read stream from Kafka
        df_raw = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKER) \
            .option("subscribe", KAFKA_TOPIC) \
            .option("startingOffsets", "latest") \
            .load()

        # Parse JSON messages
        df_parsed = df_raw.select(
            from_json(col("value").cast("string"), schema).alias("json_data")
        ).select("json_data.*")

        # Filter out invalid records
        df_clean = df_parsed.filter(
            col("event_id").isNotNull() &
            col("timestamp").isNotNull() &
            col("device_id").isNotNull() &
            col("reading_value").isNotNull()
        )

        # Validate schema
        validate_schema(df_clean)

        # Detect anomalies where reading_value > 70
        df_anomalies = df_clean.filter(col("reading_value") > 70.0)

        # Write raw streaming data to MinIO (S3)
        df_clean.writeStream \
            .format("parquet") \
            .option("checkpointLocation", "/tmp/spark-checkpoints/raw") \
            .option("path", RAW_DATA_PATH) \
            .outputMode("append") \
            .start()

        # Write anomaly data to MinIO (S3)
        df_anomalies.writeStream \
            .format("parquet") \
            .option("checkpointLocation", "/tmp/spark-checkpoints/anomalies") \
            .option("path", ANOMALY_DATA_PATH) \
            .outputMode("append") \
            .start()

        # Write anomalies to PostgreSQL
        df_anomalies.writeStream \
            .foreachBatch(lambda batch_df, batch_id: save_to_postgres(batch_df, "anomalies_stream")) \
            .outputMode("append") \
            .start()

        logging.info("Streaming job started. Processing events in real-time.")

        spark.streams.awaitAnyTermination()

    except Exception as e:
        logging.error(f"Streaming processing failed: {str(e)}")
        spark.stop()


if __name__ == "__main__":
    main()
