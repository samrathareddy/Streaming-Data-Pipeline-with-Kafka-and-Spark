import os
import json
from kafka import KafkaConsumer
from pymongo import MongoClient
from datetime import datetime

# ---------------------------
# CONFIGURATION
# ---------------------------
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor_readings")

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")
MONGODB_DB = os.getenv("MONGODB_DB", "iot_data")
MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION", "sensor_readings")

# ---------------------------
# CONNECT TO MONGODB
# ---------------------------
mongo_client = MongoClient(MONGODB_URI)
db = mongo_client[MONGODB_DB]
collection = db[MONGODB_COLLECTION]


# ---------------------------
# FUNCTION: CONSUME DATA FROM KAFKA & STORE IN MONGODB
# ---------------------------
def consume_kafka_to_mongodb():
    """
    Consumes real-time sensor data from Kafka and stores it in MongoDB.
    """
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )

    print(f"✅ Listening for real-time sensor data on Kafka topic: {KAFKA_TOPIC}")

    for message in consumer:
        data = message.value
        device_id = data.get("device_id")
        reading_value = data.get("reading_value")
        timestamp = data.get("timestamp", int(datetime.utcnow().timestamp()))

        if device_id is None or reading_value is None:
            continue  # Ignore malformed messages

        # Insert into MongoDB
        collection.insert_one({
            "device_id": device_id,
            "reading_value": reading_value,
            "timestamp": timestamp
        })

        print(f"✅ Stored in MongoDB: Device {device_id} | Reading {reading_value}")


# ---------------------------
# MAIN EXECUTION
# ---------------------------
if __name__ == "__main__":
    consume_kafka_to_mongodb()
