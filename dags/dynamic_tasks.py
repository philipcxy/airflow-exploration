from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import cross_downstream


with DAG(
    dag_id="dynamic_example",
    description="Example of creating tasks dynamically",
    schedule_interval="@daily",
    start_date=datetime(2024, 3, 1),
):
    start_tasks = [EmptyOperator(task_id=f"some_task_{i}") for i in range(3)]
    follow_up_tasks = [EmptyOperator(task_id=f"follow_up_task_{i}") for i in range(5)]

    cross_downstream(from_tasks=start_tasks, to_tasks=follow_up_tasks)
