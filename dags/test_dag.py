from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

# Define the DAG
with DAG(
    dag_id="example_dag",
    start_date=datetime(2024, 2, 24),
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    # Task 1 - Print a message
    print_message = BashOperator(
        task_id="print_message",
        bash_command="echo 'Hello, Airflow!'",
    )

    # Task 2 - Sleep for 5 seconds
    sleep_task = BashOperator(
        task_id="sleep_task",
        bash_command="sleep 5",
    )

    # Define the task flow (Task 1 -> Task 2)
    print_message >> sleep_task
