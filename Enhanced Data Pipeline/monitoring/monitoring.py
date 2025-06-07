import os
import json
import subprocess
import time
import logging
import requests

# Logging Configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Prometheus & Grafana Configurations
PROMETHEUS_CONFIG_PATH = os.getenv("PROMETHEUS_CONFIG_PATH", "/etc/prometheus/prometheus.yml")
PROMETHEUS_PORT = os.getenv("PROMETHEUS_PORT", "9090")
GRAFANA_PORT = os.getenv("GRAFANA_PORT", "3000")
GRAFANA_API_URL = f"http://localhost:{GRAFANA_PORT}/api"
GRAFANA_ADMIN_USER = os.getenv("GRAFANA_ADMIN_USER", "admin")
GRAFANA_ADMIN_PASS = os.getenv("GRAFANA_ADMIN_PASS", "admin")
DASHBOARDS_PATH = os.getenv("DASHBOARDS_PATH", "./grafana_dashboards")


def start_prometheus():
    """
    Starts Prometheus as a subprocess and ensures configuration exists.
    """
    if not os.path.exists(PROMETHEUS_CONFIG_PATH):
        logging.error(f"Prometheus config file not found: {PROMETHEUS_CONFIG_PATH}")
        return

    logging.info("Starting Prometheus...")
    try:
        subprocess.Popen(["prometheus", "--config.file", PROMETHEUS_CONFIG_PATH])
        logging.info(f"Prometheus started on port {PROMETHEUS_PORT}")
    except Exception as e:
        logging.error(f"Failed to start Prometheus: {e}")


def start_grafana():
    """
    Starts Grafana as a subprocess.
    """
    logging.info("Starting Grafana...")
    try:
        subprocess.Popen(["grafana-server"])
        logging.info(f"Grafana started on port {GRAFANA_PORT}")
    except Exception as e:
        logging.error(f"Failed to start Grafana: {e}")


def wait_for_grafana():
    """
    Waits until Grafana API is responsive before proceeding.
    """
    logging.info("Waiting for Grafana to be ready...")
    for _ in range(30):  # Wait for up to 30 seconds
        try:
            response = requests.get(f"{GRAFANA_API_URL}/health")
            if response.status_code == 200:
                logging.info("Grafana is ready.")
                return True
        except requests.ConnectionError:
            pass
        time.sleep(1)
    logging.error("Grafana did not start in time.")
    return False


def create_grafana_datasource():
    """
    Creates a Prometheus data source in Grafana via the API.
    """
    logging.info("Creating Grafana Prometheus datasource...")

    datasource_payload = {
        "name": "Prometheus",
        "type": "prometheus",
        "url": f"http://localhost:{PROMETHEUS_PORT}",
        "access": "proxy",
        "basicAuth": False
    }

    response = requests.post(
        f"{GRAFANA_API_URL}/datasources",
        auth=(GRAFANA_ADMIN_USER, GRAFANA_ADMIN_PASS),
        headers={"Content-Type": "application/json"},
        data=json.dumps(datasource_payload)
    )

    if response.status_code in [200, 201]:
        logging.info("Grafana Prometheus datasource created successfully.")
    else:
        logging.error(f"Failed to create Grafana datasource: {response.text}")


def import_grafana_dashboards():
    """
    Imports predefined Grafana dashboards from JSON files.
    """
    if not os.path.exists(DASHBOARDS_PATH):
        logging.warning(f"Dashboard directory not found: {DASHBOARDS_PATH}")
        return

    for dashboard_file in os.listdir(DASHBOARDS_PATH):
        if dashboard_file.endswith(".json"):
            dashboard_path = os.path.join(DASHBOARDS_PATH, dashboard_file)
            with open(dashboard_path, "r") as f:
                dashboard_data = json.load(f)
                dashboard_payload = {
                    "dashboard": dashboard_data,
                    "overwrite": True
                }

                response = requests.post(
                    f"{GRAFANA_API_URL}/dashboards/db",
                    auth=(GRAFANA_ADMIN_USER, GRAFANA_ADMIN_PASS),
                    headers={"Content-Type": "application/json"},
                    data=json.dumps(dashboard_payload)
                )

                if response.status_code in [200, 201]:
                    logging.info(f"Imported Grafana dashboard: {dashboard_file}")
                else:
                    logging.error(f"Failed to import {dashboard_file}: {response.text}")


def main():
    """
    Main function to start Prometheus, Grafana, and configure monitoring.
    """
    start_prometheus()
    start_grafana()

    # Wait for Grafana before setting up datasources/dashboards
    if wait_for_grafana():
        create_grafana_datasource()
        import_grafana_dashboards()

    logging.info("Monitoring setup complete.")


if __name__ == "__main__":
    main()
