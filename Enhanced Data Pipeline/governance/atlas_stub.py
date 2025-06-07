import json
import logging
import os
import requests

# Logging Configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Apache Atlas / OpenMetadata Configuration
ATLAS_API_URL = os.getenv("ATLAS_API_URL", "http://atlas:21000/api/atlas/v2/lineage")
ATLAS_USERNAME = os.getenv("ATLAS_USERNAME", "admin")
ATLAS_PASSWORD = os.getenv("ATLAS_PASSWORD", "admin")

# Headers for API requests
HEADERS = {
    "Content-Type": "application/json"
}


def check_dataset_exists(dataset_name):
    """
    Checks if a dataset exists in Apache Atlas or OpenMetadata before registering lineage.

    :param dataset_name: str, dataset qualified name (e.g., "mysql.orders")
    :return: bool, True if dataset exists, False otherwise
    """
    dataset_api_url = f"{ATLAS_API_URL}/entities?type=Dataset&name={dataset_name}"
    try:
        response = requests.get(dataset_api_url, auth=(ATLAS_USERNAME, ATLAS_PASSWORD), headers=HEADERS)
        if response.status_code == 200:
            data = response.json()
            if "entities" in data and len(data["entities"]) > 0:
                logging.info(f"Dataset '{dataset_name}' exists in Apache Atlas.")
                return True
            else:
                logging.warning(f"Dataset '{dataset_name}' does not exist in Apache Atlas.")
                return False
        else:
            logging.error(f"Failed to check dataset existence: {response.status_code} - {response.text}")
            return False
    except requests.RequestException as e:
        logging.error(f"Error while checking dataset existence: {str(e)}")
        return False


def register_dataset_lineage(source_name, target_name, extra_info=None):
    """
    Registers dataset lineage in Apache Atlas / OpenMetadata via REST API.

    :param source_name: str, qualified name of the source dataset (e.g., "mysql.orders")
    :param target_name: str, qualified name of the target dataset (e.g., "minio.raw-data.orders")
    :param extra_info: dict, additional metadata such as transformations, job details, timestamps
    """
    if not check_dataset_exists(source_name) or not check_dataset_exists(target_name):
        logging.error(f"Cannot register lineage: One or both datasets do not exist.")
        return

    lineage_payload = {
        "guidEntityMap": {},
        "relations": [
            {
                "typeName": "Process",
                "fromEntityId": source_name,
                "toEntityId": target_name,
                "relationshipAttributes": extra_info or {}
            }
        ]
    }

    try:
        response = requests.post(
            f"{ATLAS_API_URL}/entities",
            auth=(ATLAS_USERNAME, ATLAS_PASSWORD),
            headers=HEADERS,
            data=json.dumps(lineage_payload)
        )

        if response.status_code in [200, 201]:
            logging.info(f"Successfully registered lineage from '{source_name}' to '{target_name}'")
        else:
            logging.error(f"Failed to register lineage: {response.status_code} - {response.text}")

    except requests.RequestException as e:
        logging.error(f"Error while registering dataset lineage: {str(e)}")


if __name__ == "__main__":
    register_dataset_lineage(
        "mysql.orders",
        "minio.raw-data.orders",
        {"job": "batch_ingestion_dag", "transformation": "cleaning, enrichment"}
    )
