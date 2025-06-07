import json
import time
import random
import uuid
import logging
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaError
import os

# Logging Configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Kafka Configuration (Can be overridden using environment variables)
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor_readings")
MESSAGE_FREQUENCY = float(os.getenv("MESSAGE_FREQUENCY", 1))  # Delay between messages (seconds)
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 10))  # Number of messages per batch
ACKS_MODE = os.getenv("ACKS_MODE", "all")  # Kafka acknowledgments ("all", "1", or "0")

# Kafka Producer Initialization
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks=ACKS_MODE
)


def create_kafka_topic(topic_name):
    """
    Creates a Kafka topic if it does not exist.
    """
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
        existing_topics = admin_client.list_topics()

        if topic_name not in existing_topics:
            topic = NewTopic(name=topic_name, num_partitions=3, replication_factor=1)
            admin_client.create_topics([topic])
            logging.info(f"Kafka topic '{topic_name}' created successfully.")
        else:
            logging.info(f"Kafka topic '{topic_name}' already exists.")
    except KafkaError as e:
        logging.error(f"Failed to create Kafka topic '{topic_name}': {e}")


def generate_event():
    """
    Generates a single random sensor event.
    """
    return {
        "event_id": str(uuid.uuid4()),
        "timestamp": int(time.time()),
        "device_id": random.randint(1000, 9999),
        "reading_value": round(random.uniform(20.0, 80.0), 2)
    }


def produce_messages():
    """
    Continuously produces sensor messages to Kafka in batches.
    """
    logging.info(f"Starting Kafka Producer. Sending messages to topic: {KAFKA_TOPIC}")
    create_kafka_topic(KAFKA_TOPIC)

    batch = []
    message_count = 0

    try:
        while True:
            event = generate_event()
            batch.append(event)

            # Send batch if batch size is reached
            if len(batch) >= BATCH_SIZE:
                for msg in batch:
                    producer.send(KAFKA_TOPIC, msg)
                producer.flush()
                logging.info(f"Sent batch of {len(batch)} messages to Kafka topic: {KAFKA_TOPIC}")
                message_count += len(batch)
                batch.clear()

            # Print message details for debugging
            logging.debug(f"Generated event: {event}")

            # Sleep to control message frequency
            time.sleep(MESSAGE_FREQUENCY)

    except KeyboardInterrupt:
        logging.warning("Kafka Producer stopped manually.")
    except KafkaError as e:
        logging.error(f"Kafka producer error: {e}")
    finally:
        logging.info(f"Kafka Producer shutting down. Total messages sent: {message_count}")
        producer.close()


if __name__ == "__main__":
    produce_messages()
