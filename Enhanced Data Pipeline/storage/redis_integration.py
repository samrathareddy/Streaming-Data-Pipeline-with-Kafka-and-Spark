import os
import json
import redis
from kafka import KafkaConsumer, KafkaProducer
from feast import FeatureStore

# ---------------------------
# CONFIGURATION
# ---------------------------
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_INPUT_TOPIC = os.getenv("KAFKA_INPUT_TOPIC", "sensor_readings")
KAFKA_OUTPUT_TOPIC = os.getenv("KAFKA_OUTPUT_TOPIC", "processed_readings")

FEAST_REPO_PATH = os.getenv("FEAST_REPO_PATH", "./feature_repo")

# ---------------------------
# CONNECT TO REDIS
# ---------------------------
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

# ---------------------------
# CONNECT TO FEATURE STORE (FEAST)
# ---------------------------
store = FeatureStore(repo_path=FEAST_REPO_PATH)

# ---------------------------
# KAFKA PRODUCER FOR PROCESSED MESSAGES
# ---------------------------
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


# ---------------------------
# FUNCTION: FETCH FEATURES FROM FEAST & CACHE IN REDIS
# ---------------------------
def get_features_from_feast(device_id):
    """
    Retrieves real-time features for a given device_id from Feast and caches them in Redis.
    """
    cache_key = f"features:{device_id}"
    cached_features = redis_client.get(cache_key)

    if cached_features:
        print(f"✅ Cache Hit: Features for device {device_id} retrieved from Redis")
        return json.loads(cached_features)

    # Fetch from Feast if not in cache
    feature_refs = ["device_features:avg_reading", "device_features:max_reading"]
    feature_vector = store.get_online_features(
        features=feature_refs,
        entity_rows=[{"device_id": device_id}]
    ).to_dict()

    # Convert to JSON
    features = {
        "avg_reading": feature_vector["device_features:avg_reading"][0],
        "max_reading": feature_vector["device_features:max_reading"][0]
    }

    # Store in Redis cache (expires in 1 hour)
    redis_client.setex(cache_key, 3600, json.dumps(features))
    print(f"✅ Cache Miss: Features for device {device_id} fetched from Feast and stored in Redis")

    return features


# ---------------------------
# FUNCTION: PROCESS STREAMING DATA & STORE IN REDIS QUEUE
# ---------------------------
def process_streaming_data():
    """
    Consumes Kafka sensor readings, fetches ML features, processes data, and stores in Redis.
    """
    consumer = KafkaConsumer(
        KAFKA_INPUT_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )

    print(f"✅ Listening for sensor data on Kafka topic: {KAFKA_INPUT_TOPIC}")

    for message in consumer:
        data = message.value
        device_id = data.get("device_id")
        reading_value = data.get("reading_value")

        if device_id is None or reading_value is None:
            continue  # Ignore malformed messages

        # Fetch cached ML features
        features = get_features_from_feast(device_id)

        # Process the data (Example: Normalize reading)
        normalized_reading = reading_value / features["max_reading"]

        processed_data = {
            "device_id": device_id,
            "original_reading": reading_value,
            "normalized_reading": round(normalized_reading, 4),
            "avg_reading": features["avg_reading"]
        }

        # Store in Redis Queue (List)
        redis_client.lpush("processed_queue", json.dumps(processed_data))
        print(f"✅ Stored in Redis Queue: {processed_data}")

        # Send processed data to Kafka
        producer.send(KAFKA_OUTPUT_TOPIC, processed_data)
        print(f"✅ Sent to Kafka topic {KAFKA_OUTPUT_TOPIC}: {processed_data}")


# ---------------------------
# FUNCTION: FETCH PROCESSED DATA FROM REDIS QUEUE
# ---------------------------
def fetch_processed_data():
    """
    Retrieves processed data from Redis queue.
    """
    queue_length = redis_client.llen("processed_queue")

    if queue_length == 0:
        print("❌ No processed data available in Redis queue.")
        return None

    processed_data = redis_client.rpop("processed_queue")
    return json.loads(processed_data) if processed_data else None


# ---------------------------
# MAIN EXECUTION
# ---------------------------
if __name__ == "__main__":
    process_streaming_data()
