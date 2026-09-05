from datetime import datetime
import requests
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator


CONNECT_URL = "http://connect:8083"
CONNECTOR_NAME = "postgres-cdc"


def check_kafka_connect():
    response = requests.get(
        f"{CONNECT_URL}/",
        timeout=10,
    )

    response.raise_for_status()

    print("Kafka Connect is available")


def check_debezium_connector():
    response = requests.get(
        f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}/status",
        timeout=10,
    )

    response.raise_for_status()

    status = response.json()

    connector_state = status["connector"]["state"]

    if connector_state != "RUNNING":
        raise RuntimeError(
            f"Debezium connector is not RUNNING: {connector_state}"
        )

    for task in status["tasks"]:
        if task["state"] != "RUNNING":
            raise RuntimeError(
                f"Debezium task {task['id']} is not RUNNING: "
                f"{task['state']}"
            )

    print("Debezium connector is RUNNING")
    print(f"Connector: {CONNECTOR_NAME}")


with DAG(
    dag_id="cdc_health_check",
    start_date=datetime(2026, 8, 27),
    schedule=None,
    catchup=False,
    tags=["cdc", "debezium", "kafka", "monitoring"],
) as dag:

    kafka_connect_check = PythonOperator(
        task_id="check_kafka_connect",
        python_callable=check_kafka_connect,
    )

    debezium_check = PythonOperator(
        task_id="check_debezium_connector",
        python_callable=check_debezium_connector,
    )

    kafka_connect_check >> debezium_check
