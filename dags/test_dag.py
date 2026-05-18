"""Premier DAG de test pour comprendre PythonOperator et les dependances."""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import logging


def goodbye():
    logging.info("aurevoiiiir")


def greeting():
    logging.info("bonjouuuur")


dag = DAG(
    dag_id="simple_dag",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
)

greeting_task = PythonOperator(
    task_id="greeting_task",
    python_callable=greeting,
    dag=dag,
)

aurevoir_task = PythonOperator(
    task_id="aurevoir_task",
    python_callable=goodbye,
    dag=dag,
)

# Airflow execute greeting_task avant aurevoir_task.
greeting_task >> aurevoir_task
