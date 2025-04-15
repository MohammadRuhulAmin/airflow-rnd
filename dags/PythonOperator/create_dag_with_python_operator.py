from airflow import DAG 
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

default_args={
    'owner':'ruhul',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def greet():
    print('Greetings from Python')

def treat():
    print('Treating from Python')

def variable_greet( age,ti):
    name = ti.xcom_pull(task_ids='get_name')
    print(f'name: {name}, age:{age}')

def variable_treet(name, age):
    print(f'name:{name} age:{age}')

def get_name():
    return 'Jerry'

def get_json():
    return {
        'name':'ruhul',
        'email':'email@avc.com'
    }

with DAG(
    default_args = default_args,
    dag_id='dig_id_v_x_1',
    start_date = datetime(2025, 4, 14),
    schedule_interval='@daily',
    catchup=False
) as dag:
    task0 = PythonOperator(
        task_id='greet-0',
        python_callable=greet
    )

    task1 = PythonOperator(
        task_id='greet-1',
        python_callable=variable_greet,
        op_kwargs={'age':25}
    )


    task2 = PythonOperator(
        task_id = 'greet-2',
        python_callable=variable_treet,
        op_kwargs={'name':'Gop','age':21}
    )

    task3 = PythonOperator(
        task_id='get_name',
        python_callable=get_name

    )

    task4= PythonOperator(
        task_id='get_json',
        python_callable=get_json
    )

    
    task3 >> task1 >> task2 >> task0 >> task4