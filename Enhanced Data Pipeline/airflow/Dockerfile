FROM apache/airflow:2.5.0-python3.9

USER root
RUN apt-get update && apt-get install -y gcc libpq-dev

USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# The default entrypoint/cmd from apache/airflow will start the scheduler/webserver