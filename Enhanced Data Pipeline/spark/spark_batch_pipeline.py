import os
import logging
import glob
import shutil
import sys
import great_expectations as ge
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp, col, sum as spark_sum, count, when, lit
)
from pyspark.sql.utils import AnalysisException

# Logging Configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# MinIO (S3-compatible) Configuration (Replace with your own settings)
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
RAW_DATA_PATH = "s3a://raw-data/orders/orders.csv"
PROCESSED_DATA_PATH = "s3a://processed-data/orders_transformed.csv"

# Local file output path
LOCAL_OUTPUT_DIR = "/tmp/transformed_orders"
LOCAL_OUTPUT_FILE = "/tmp/transformed_orders.csv"


def validate_schema(df):
    """
    Validate the schema of the incoming DataFrame using Great Expectations.
    Ensures:
      - 'order_id' is non-null and unique
      - 'customer_id' is non-null and positive
      - 'amount' is non-null and greater than zero
    """
    logging.info("Validating schema using Great Expectations...")

    ge_df = ge.from_pandas(df.toPandas())  # Convert Spark DataFrame to Pandas for GE validation

    # Expect order_id to be unique and non-null
    result_order_id = ge_df.expect_column_values_to_not_be_null("order_id")
    if not result_order_id.success:
        raise ValueError("Validation failed: 'order_id' contains null values")

    # Expect customer_id to be non-null and positive
    result_customer_id = ge_df.expect_column_values_to_be_between("customer_id", min_value=1)
    if not result_customer_id.success:
        raise ValueError("Validation failed: 'customer_id' is not positive")

    # Expect amount to be non-null and greater than 0
    result_amount = ge_df.expect_column_values_to_be_between("amount", min_value=0.01)
    if not result_amount.success:
        raise ValueError("Validation failed: 'amount' contains zero or negative values")

    logging.info("Schema validation passed.")


def main():
    """
    Batch ETL Pipeline:
    - Reads raw CSV data from MinIO (S3-compatible)
    - Performs schema validation with Great Expectations
    - Cleans and transforms data:
      - Drops duplicates
      - Fills null values
      - Adds timestamp column
      - Aggregates total order amount per customer
    - Saves transformed data to MinIO (S3) and local storage
    """
    try:
        # Initialize Spark session
        spark = SparkSession.builder \
            .appName("BatchETL") \
            .getOrCreate()

        # Configure Spark to access MinIO using s3a
        spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", MINIO_ENDPOINT)
        spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", MINIO_ACCESS_KEY)
        spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", MINIO_SECRET_KEY)
        spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")

        logging.info(f"Reading raw data from MinIO ({RAW_DATA_PATH})...")

        # Read CSV file from MinIO
        df = spark.read.option("header", "true").csv(RAW_DATA_PATH)

        # Check if the DataFrame is empty
        if df.isEmpty():
            raise Exception("No data found in the input file.")

        # Convert data types for safer transformations
        df = df.withColumn("order_id", col("order_id").cast("int")) \
               .withColumn("customer_id", col("customer_id").cast("int")) \
               .withColumn("amount", col("amount").cast("double"))

        # Validate schema
        validate_schema(df)

        # Print number of records before transformation
        initial_record_count = df.count()
        logging.info(f"Initial record count: {initial_record_count}")

        # 1) Drop duplicate order IDs
        df = df.dropDuplicates(["order_id"])

        # 2) Fill null amounts with 0.0 (just an example - real logic may differ)
        df = df.fillna({"amount": 0.0})

        # 3) Add a timestamp column for processing time
        df_transformed = df.withColumn("processed_timestamp", current_timestamp())

        # 4) Aggregate data: Total amount per customer
        df_aggregated = df_transformed.groupBy("customer_id").agg(
            spark_sum("amount").alias("total_spent"),
            count("*").alias("order_count")
        )

        # Print number of records after transformation
        final_record_count = df_transformed.count()
        logging.info(f"Final record count: {final_record_count}")

        # Write transformed data to MinIO
        logging.info(f"Writing transformed data to MinIO ({PROCESSED_DATA_PATH})...")
        df_transformed.write.mode("overwrite").option("header", "true").csv(PROCESSED_DATA_PATH)

        # Write transformed data to local CSV (coalesced to 1 file)
        logging.info(f"Writing transformed data to local directory ({LOCAL_OUTPUT_DIR})...")
        df_transformed.coalesce(1).write.mode("overwrite").option("header", "true").csv(LOCAL_OUTPUT_DIR)

        # Rename the part file to a single CSV for Airflow compatibility
        csv_files = glob.glob(f"{LOCAL_OUTPUT_DIR}/part-*.csv")
        if csv_files:
            shutil.move(csv_files[0], LOCAL_OUTPUT_FILE)
            logging.info(f"Transformed file successfully written to {LOCAL_OUTPUT_FILE}")
        else:
            logging.warning("No CSV files found after transformation.")

        # Stop Spark session
        spark.stop()
        logging.info("Batch ETL process completed successfully.")

    except AnalysisException as e:
        logging.error(f"Spark DataFrame operation failed: {str(e)}")
        sys.exit(1)

    except Exception as e:
        logging.error(f"Batch processing failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
