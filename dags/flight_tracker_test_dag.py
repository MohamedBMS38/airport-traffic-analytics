from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging


def start_pipeline(): 
    logging.info("Début du pipeline Flight Tracker")

def extract_data():
    logging.info("Extraction des données OpenSky simulée")

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

start_task >> extract_task >> end_task
