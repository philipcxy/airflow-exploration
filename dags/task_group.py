from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

with DAG(
    dag_id="task_group_dag",
    description="Example of using task groups to control flow",
    schedule_interval="@daily",
    start_date=datetime(2024, 3, 1),
):
    with TaskGroup(group_id="task_group_1") as at_least_one_success:
        all_tasks_finished = EmptyOperator(
            task_id="all_tasks_completed", trigger_rule=TriggerRule.ALL_DONE
        )
        one_success = EmptyOperator(
            task_id="one_task_success", trigger_rule=TriggerRule.ONE_SUCCESS
        )

        both_conditions_met = EmptyOperator(
            task_id="continue_conditions_met", trigger_rule=TriggerRule.ALL_SUCCESS
        )

        [all_tasks_finished, one_success] >> both_conditions_met

    from_tasks = [EmptyOperator(task_id=f"from_task_{i}") for i in range(2)]
    to_tasks = [EmptyOperator(task_id=f"to_task_{i}") for i in range(3)]

    from_tasks >> at_least_one_success >> to_tasks
