import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import requests
import json

# Database Configuration (PostgreSQL)
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "processed_db")
DB_USER = os.getenv("DB_USER", "user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "pass")

# Tableau API Configuration
TABLEAU_SERVER = os.getenv("TABLEAU_SERVER", "https://your-tableau-server.com")
TABLEAU_SITE_ID = os.getenv("TABLEAU_SITE_ID", "")
TABLEAU_USERNAME = os.getenv("TABLEAU_USERNAME", "admin")
TABLEAU_PASSWORD = os.getenv("TABLEAU_PASSWORD", "admin")
TABLEAU_PROJECT_ID = os.getenv("TABLEAU_PROJECT_ID", "project_uuid")

# Looker API Configuration
LOOKER_API_URL = os.getenv("LOOKER_API_URL", "https://your-looker-instance.com")
LOOKER_CLIENT_ID = os.getenv("LOOKER_CLIENT_ID", "your_client_id")
LOOKER_CLIENT_SECRET = os.getenv("LOOKER_CLIENT_SECRET", "your_client_secret")

# Power BI Configuration
POWER_BI_WORKSPACE_ID = os.getenv("POWER_BI_WORKSPACE_ID", "workspace_uuid")
POWER_BI_DATASET_ID = os.getenv("POWER_BI_DATASET_ID", "dataset_uuid")
POWER_BI_ACCESS_TOKEN = os.getenv("POWER_BI_ACCESS_TOKEN", "your_access_token")


def fetch_data():
    """
    Fetch transformed data from PostgreSQL and save as CSV for BI tools.
    """
    try:
        engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
        query = "SELECT * FROM orders_transformed;"
        df = pd.read_sql(query, con=engine)

        output_file = "bi_data/orders_transformed.csv"
        df.to_csv(output_file, index=False)
        print(f"✅ Data exported for BI tools: {output_file}")

    except Exception as e:
        print(f"❌ Error fetching data from database: {e}")


def upload_to_tableau():
    """
    Upload dataset to Tableau using REST API.
    """
    try:
        # Get authentication token
        auth_payload = {
            "credentials": {
                "name": TABLEAU_USERNAME,
                "password": TABLEAU_PASSWORD,
                "site": {"contentUrl": TABLEAU_SITE_ID},
            }
        }
        auth_response = requests.post(
            f"{TABLEAU_SERVER}/api/3.9/auth/signin", json=auth_payload
        )
        auth_response.raise_for_status()
        token = auth_response.json()["credentials"]["token"]

        # Upload CSV file to Tableau
        with open("bi_data/orders_transformed.csv", "rb") as f:
            response = requests.post(
                f"{TABLEAU_SERVER}/api/3.9/sites/{TABLEAU_PROJECT_ID}/datasources",
                headers={"X-Tableau-Auth": token},
                files={"file": f},
            )
            response.raise_for_status()

        print("✅ Data uploaded to Tableau successfully.")

    except Exception as e:
        print(f"❌ Error uploading data to Tableau: {e}")


def upload_to_looker():
    """
    Upload dataset to Looker via API.
    """
    try:
        # Authenticate
        auth_payload = {"client_id": LOOKER_CLIENT_ID, "client_secret": LOOKER_CLIENT_SECRET}
        auth_response = requests.post(f"{LOOKER_API_URL}/login", data=auth_payload)
        auth_response.raise_for_status()
        token = auth_response.json()["access_token"]

        # Upload CSV to Looker
        with open("bi_data/orders_transformed.csv", "rb") as f:
            response = requests.post(
                f"{LOOKER_API_URL}/upload-data",
                headers={"Authorization": f"Bearer {token}"},
                files={"file": f},
            )
            response.raise_for_status()

        print("✅ Data uploaded to Looker successfully.")

    except Exception as e:
        print(f"❌ Error uploading data to Looker: {e}")


def upload_to_power_bi():
    """
    Upload dataset to Power BI via API.
    """
    try:
        # Prepare dataset schema
        dataset_payload = {
            "name": "Orders Transformed",
            "tables": [
                {
                    "name": "orders",
                    "columns": [
                        {"name": "order_id", "dataType": "Int64"},
                        {"name": "customer_id", "dataType": "Int64"},
                        {"name": "amount", "dataType": "Double"},
                        {"name": "processed_timestamp", "dataType": "DateTime"},
                    ],
                }
            ],
        }

        # Create dataset
        headers = {"Authorization": f"Bearer {POWER_BI_ACCESS_TOKEN}", "Content-Type": "application/json"}
        dataset_response = requests.post(
            f"https://api.powerbi.com/v1.0/myorg/groups/{POWER_BI_WORKSPACE_ID}/datasets",
            headers=headers,
            json=dataset_payload,
        )
        dataset_response.raise_for_status()

        print("✅ Data uploaded to Power BI successfully.")

    except Exception as e:
        print(f"❌ Error uploading data to Power BI: {e}")


if __name__ == "__main__":
    # Fetch data from PostgreSQL and export for BI tools
    fetch_data()

    # Upload to various BI tools
    upload_to_tableau()
    upload_to_looker()
    upload_to_power_bi()
