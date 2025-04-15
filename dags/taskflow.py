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
        bash_command='echo task-1 Executed!'  
    )
    task_2 = BashOperator(
        task_id='second_task',
        bash_command='echo task-2 Executed!'
    )
    task_3 = BashOperator(
        task_id = 'third_task',
        bash_command='echo task-3 Executed!'
    )
    task_4 = BashOperator(
        task_id = 'forth_task',
        bash_command = 'echo task-4 Executed!'
    )
    task_5 = BashOperator(
        task_id = 'fifth_task',
        bash_command = 'echo task-5 Executed!'
    )
    # Method - 1 
    # task_1.set_downstream(task_2)
    # task_1.set_downstream(task_3)
    # task_2.set_downstream(task_4)
    # task_2.set_downstream(task_5)

    # Method - 2
    task_1 >> [task_2, task_3]
    task_2 >> [task_4, task_3]
