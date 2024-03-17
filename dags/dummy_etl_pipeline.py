from datetime import datetime
from random import random

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator


from common.callbacks import failure_callback

with DAG(
    dag_id="data_pipeline",
    description="Dummy ETL pipeline",
    schedule="@daily",
    start_date=datetime(2024, 3, 1),
    catchup=False,
    on_failure_callback=failure_callback,
):

    def extract_data():
        # Load data from somewhere and get results. Let it fail sometimes too
        number = random()
        if number < 0.2:
            raise Exception()

    def transform_data():
        return True

    def load_data():
        # Save to a DB
        return True

    extract_task = PythonOperator(task_id="extract_task", python_callable=extract_data)
    tranform_task = EmptyOperator(task_id="transform_task")
    load_to_db_task = EmptyOperator(task_id="load_db_task")
    load_to_archive_task = EmptyOperator(task_id="load_archive_task")

    extract_task >> tranform_task >> [load_to_db_task, load_to_archive_task]
