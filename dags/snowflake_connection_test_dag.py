from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import logging
import os

import snowflake.connector


def test_snowflake_connection():
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )

    cursor = None

    try:
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_VERSION()")
        result = cursor.fetchone()
        logging.info("Snowflake connection OK. Version: %s", result[0])
    finally:
        if cursor:
            cursor.close()
        conn.close()


dag = DAG(
    dag_id="snowflake_connection_test_dag",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
)

test_connection_task = PythonOperator(
    task_id="test_snowflake_connection",
    python_callable=test_snowflake_connection,
    dag=dag,
)
