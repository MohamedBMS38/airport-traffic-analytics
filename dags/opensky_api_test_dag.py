from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import logging
import requests


# URL de l'endpoint OpenSky qui retourne l'etat actuel des avions en vol.
OPENSKY_URL = "https://opensky-network.org/api/states/all"


def extract_opensky_data():
    # Point de depart visible dans les logs Airflow pour suivre l'execution.
    logging.info("Début extraction OpenSky")

    # Appel HTTP vers l'API OpenSky.
    # Le timeout evite que la task reste bloquee si l'API ne repond pas.
    response = requests.get(OPENSKY_URL, timeout=30)

    # Pour l'instant, on log uniquement le statut HTTP afin de valider la connexion.
    logging.info("Code HTTP reçu : %s", response.status_code)


dag = DAG(
    dag_id="opensky_api_test_dag",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
)

extract_task = PythonOperator(
    task_id="extract_opensky_data",
    python_callable=extract_opensky_data,
    dag=dag,
)
