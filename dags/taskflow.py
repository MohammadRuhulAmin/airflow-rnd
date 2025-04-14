from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'ruhul',
    'retries': 4,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='dag_a_v_2',
    default_args=default_args,
    description='Example 1',
    start_date=datetime(2025, 4, 14),
    schedule_interval='@daily' 
) as dag:

    task_1 = BashOperator(
        task_id='first_task',  
        bash_command='echo Hellow World!'  
    )
    task_2 = BashOperator(
        task_id='second_task',
        bash_command='echo "Hi this is ruhul amin"'
    )

    task_1.set_downstream(task_2)

