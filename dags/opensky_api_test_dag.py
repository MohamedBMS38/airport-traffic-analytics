"""DAG de test simple pour valider un appel HTTP vers OpenSky."""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import logging

import requests


# Endpoint public utilise au debut du projet pour valider requests + Airflow.
OPENSKY_URL = "https://opensky-network.org/api/states/all"


def extract_opensky_data():
    # Message de depart visible dans les logs Airflow.
    logging.info("Debut extraction OpenSky")

    # Le timeout evite que la task reste bloquee si l'API ne repond pas.
    response = requests.get(OPENSKY_URL, timeout=30)

    # Pour ce DAG de test, le code HTTP suffit a valider la connexion.
    logging.info("Code HTTP recu : %s", response.status_code)


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
