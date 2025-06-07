import os
import pandas as pd
from pymongo import MongoClient
from hdfs import InsecureClient
from datetime import datetime

# ---------------------------
# CONFIGURATION
# ---------------------------
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")
MONGODB_DB = os.getenv("MONGODB_DB", "iot_data")
MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION", "sensor_readings")

HDFS_NAMENODE = os.getenv("HDFS_NAMENODE", "http://localhost:50070")
HDFS_USER = os.getenv("HDFS_USER", "hadoop")
HDFS_PATH = "/user/hadoop/iot_data"

# ---------------------------
# CONNECT TO MONGODB
# ---------------------------
mongo_client = MongoClient(MONGODB_URI)
db = mongo_client[MONGODB_DB]
collection = db[MONGODB_COLLECTION]

# ---------------------------
# CONNECT TO HDFS
# ---------------------------
hdfs_client = InsecureClient(HDFS_NAMENODE, user=HDFS_USER)


# ---------------------------
# FUNCTION: EXTRACT FROM MONGODB & UPLOAD TO HDFS
# ---------------------------
def extract_from_mongodb_to_hdfs():
    """
    Extracts batch data from MongoDB, converts to CSV, and stores it in Hadoop HDFS.
    """
    # Fetch data from MongoDB
    data = list(collection.find({}, {"_id": 0}))  # Exclude MongoDB ID field

    if not data:
        print("❌ No data available in MongoDB for extraction.")
        return

    df = pd.DataFrame(data)

    # Save as CSV
    csv_filename = f"iot_data_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(csv_filename, index=False)
    print(f"✅ Extracted data from MongoDB and saved as {csv_filename}")

    # Upload CSV to HDFS
    hdfs_file_path = f"{HDFS_PATH}/{csv_filename}"
    with hdfs_client.write(hdfs_file_path, encoding="utf-8") as writer:
        df.to_csv(writer, index=False)

    print(f"✅ Uploaded {csv_filename} to HDFS at {hdfs_file_path}")


# ---------------------------
# MAIN EXECUTION
# ---------------------------
if __name__ == "__main__":
    extract_from_mongodb_to_hdfs()
