"""DAG pedagogique pour comprendre l'ordre d'execution des tasks Airflow."""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import logging


def start_pipeline():
    logging.info("Debut du pipeline Flight Tracker")


def extract_data():
    logging.info("Extraction des donnees OpenSky simulee")


def end_pipeline():
    logging.info("Fin du pipeline Flight Tracker")


dag = DAG(
    dag_id="flight_tracker_test_dag",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
)

start_task = PythonOperator(
    task_id="start_task",
    python_callable=start_pipeline,
    dag=dag,
)

extract_task = PythonOperator(
    task_id="extract_task",
    python_callable=extract_data,
    dag=dag,
)

end_task = PythonOperator(
    task_id="end_task",
    python_callable=end_pipeline,
    dag=dag,
)

# La fleche impose l'ordre : start_task, puis extract_task, puis end_task.
start_task >> extract_task >> end_task
