from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import logging
from kafka import KafkaAdminClient, KafkaConsumer
from kafka.admin import NewTopic
from kafka.errors import KafkaError

# Kafka Configuration
KAFKA_BROKER = "kafka:9092"  # Replace with your Kafka broker if needed
KAFKA_TOPIC = "sensor_readings"
CONSUMER_GROUP = "sensor_readings_consumer"
LAG_THRESHOLD = 10  # Adjust this threshold as per business needs


def check_kafka_consumer_lag():
    """
    Connects to Kafka, retrieves consumer group lag, and logs warnings if lag exceeds threshold.
    """

    try:
        logging.info(f"Connecting to Kafka broker: {KAFKA_BROKER}")

        # Initialize Kafka Admin Client
        admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)

        # Fetch Consumer Group Offsets
        consumer_offsets = admin_client.list_consumer_groups()
        consumer_group_info = admin_client.describe_consumer_groups([CONSUMER_GROUP])

        logging.info(f"Consumer groups: {consumer_offsets}")

        for group in consumer_group_info:
            logging.info(
                f"Consumer Group: {group['group_id']}, State: {group['state']}, Members: {len(group['members'])}")

        # Fetch Kafka Partitions and Offsets
        consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=KAFKA_BROKER, group_id=CONSUMER_GROUP,
                                 enable_auto_commit=False)
        partitions = consumer.partitions_for_topic(KAFKA_TOPIC)

        logging.info(f"Partitions for topic '{KAFKA_TOPIC}': {partitions}")

        for partition in partitions:
            topic_partition = f"{KAFKA_TOPIC}-{partition}"
            committed_offset = consumer.committed(topic_partition)

            if committed_offset is not None:
                log_message = f"Partition {partition}: Committed Offset = {committed_offset}"
                logging.info(log_message)

                # Fetch the latest offset
                end_offsets = consumer.end_offsets([topic_partition])
                latest_offset = end_offsets.get(topic_partition, 0)

                # Calculate Lag
                lag = latest_offset - committed_offset
                logging.info(f"Partition {partition}: Latest Offset = {latest_offset}, Consumer Lag = {lag}")

                # Raise alert if lag exceeds threshold
                if lag > LAG_THRESHOLD:
                    logging.warning(f"ALERT! Consumer lag detected on {topic_partition}. Lag: {lag}")

            else:
                logging.warning(f"No committed offset found for partition {partition}")

        logging.info("Kafka consumer lag monitoring complete.")

    except KafkaError as e:
        logging.error(f"Kafka connection error: {str(e)}")

    except Exception as ex:
        logging.error(f"Error while monitoring Kafka consumer lag: {str(ex)}")


# Airflow DAG Configuration
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1
}

with DAG(
        dag_id='streaming_monitoring_dag',
        default_args=default_args,
        schedule_interval='@hourly',
        catchup=False
) as dag:
    monitor_kafka_task = PythonOperator(
        task_id='monitor_kafka_consumer_lag',
        python_callable=check_kafka_consumer_lag
    )
