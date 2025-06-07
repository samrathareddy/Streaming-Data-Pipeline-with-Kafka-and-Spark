from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
import pandas as pd
import boto3
import logging

# Great Expectations
import great_expectations as ge

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1
}


def extract_data_from_mysql(**kwargs):
    """
    Extract batch data from MySQL.
    Pulls data from the 'orders' table and writes it to /tmp/orders.csv.
    """
    logging.info("Starting extraction from MySQL...")
    mysql_hook = MySqlHook(mysql_conn_id="mysql_default")
    df = mysql_hook.get_pandas_df("SELECT * FROM orders;")
    df.to_csv("/tmp/orders.csv", index=False)
    logging.info(f"Extracted {len(df)} records from MySQL and saved to /tmp/orders.csv.")


def validate_data_with_ge(**kwargs):
    """
    Validate extracted data with Great Expectations.
    - Ensures 'order_id' is not null
    - Ensures 'amount' is not zero or negative
    """
    logging.info("Running Great Expectations validation on /tmp/orders.csv...")
    df = pd.read_csv("/tmp/orders.csv")
    ge_df = ge.from_pandas(df)

    # 1) Expect no null in 'order_id'
    result_order_id = ge_df.expect_column_values_to_not_be_null("order_id")
    if not result_order_id["success"]:
        raise ValueError("Data validation failed: 'order_id' has null values")

    # 2) Expect 'amount' to be strictly greater than 0
    result_amount = ge_df.expect_column_values_to_be_between("amount", 0.01, 9999999, strictly=True)
    if not result_amount["success"]:
        raise ValueError("Data validation failed: 'amount' is not strictly positive")

    logging.info("Great Expectations validations passed.")


def load_to_minio(**kwargs):
    """
    Load raw data CSV to MinIO (S3-compatible).
    Bucket: 'raw-data'
    Key: 'orders/orders.csv'
    """
    logging.info("Uploading CSV to MinIO (raw-data bucket)...")
    s3 = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='minio',
        aws_secret_access_key='minio123',
        region_name='us-east-1'
    )
    bucket_name = "raw-data"
    # Attempt to create bucket if not exists
    try:
        s3.create_bucket(Bucket=bucket_name)
        logging.info(f"Bucket '{bucket_name}' created or already exists.")
    except Exception as e:
        logging.info(f"Bucket creation skipped (possibly exists): {e}")

    # Upload file
    s3.upload_file("/tmp/orders.csv", bucket_name, "orders/orders.csv")
    logging.info("File successfully uploaded to MinIO.")


def load_to_postgres(**kwargs):
    """
    Load final transformed data into Postgres table 'orders_transformed'.
    Assumes Spark job wrote /tmp/transformed_orders.csv locally.

    Table schema:
    - order_id INT
    - customer_id INT
    - amount DECIMAL(10,2)
    - processed_timestamp TIMESTAMP
    """
    logging.info("Starting load into Postgres from /tmp/transformed_orders.csv...")
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    df = pd.read_csv("/tmp/transformed_orders.csv")

    pg_hook.run("CREATE TABLE IF NOT EXISTS orders_transformed ( \
        order_id INT, \
        customer_id INT, \
        amount DECIMAL(10,2), \
        processed_timestamp TIMESTAMP );")

    # Clear table before load
    pg_hook.run("TRUNCATE TABLE orders_transformed;")

    # Insert row by row (simple approach)
    rows_loaded = 0
    for _, row in df.iterrows():
        insert_sql = """
        INSERT INTO orders_transformed(order_id, customer_id, amount, processed_timestamp)
        VALUES (%s, %s, %s, %s)
        """
        pg_hook.run(insert_sql, parameters=(
            row['order_id'],
            row['customer_id'],
            row['amount'],
            row['processed_timestamp']
        ))
        rows_loaded += 1

    logging.info(f"Finished loading {rows_loaded} records into Postgres (orders_transformed).")


with DAG(
        dag_id='batch_ingestion_dag',
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False
) as dag:
    extract_task = PythonOperator(
        task_id='extract_mysql',
        python_callable=extract_data_from_mysql
    )

    validate_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data_with_ge
    )

    load_to_minio_task = PythonOperator(
        task_id='load_to_minio',
        python_callable=load_to_minio
    )

    # trigger Spark job with BashOperator
    spark_transform_task = BashOperator(
        task_id='spark_transform',
        bash_command='spark-submit --master local[2] /opt/spark_jobs/spark_batch_job.py'
    )

    load_postgres_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres
    )

    # Define task dependencies
    extract_task >> validate_task >> load_to_minio_task >> spark_transform_task >> load_postgres_task
