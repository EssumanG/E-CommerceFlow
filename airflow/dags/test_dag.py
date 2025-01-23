from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'gkessuman',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


with DAG(
    dag_id="dag_test_with_bashOperators",
    default_args=default_args,
    description="A dag that test using bash operators",
    start_date=datetime(2025, 1, 20, 6),
    schedule_interval="@daily"
) as dag:
    task1=BashOperator(
        task_id="first_task",
        bash_command="echo hey, I am task1 and I will be the first task to run"
    )
    task2 = BashOperator(
        task_id="second_task",
        bash_command="echo hey, I am task2 and I will be running after task1"
    )
    task3 = BashOperator(
        task_id="third_task",
        bash_command="echo hey, I am task3 and I will be running after task1"
    )

    task4 = BashOperator(
        task_id="fourth_and_final_task",
        bash_command="echo hey, I am task4 and I will be final task after both task2 and task3"
    )


    task1 >> [task2, task3]
    task2 >> task4
    task3 >> task4