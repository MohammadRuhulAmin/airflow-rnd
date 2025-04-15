from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    'owner': 'ruhul',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    dag_id='taskflow_api_v1',
    default_args=default_args,
    start_date=datetime(2025, 4, 15),
    schedule_interval='@daily',
    catchup=False,
    tags=['example']
)
def hellow_world_etl():
    
    @task()
    def get_name():
        return 'Ruhul'

    @task()
    def get_age():
        return 29

    @task()
    def greet(name: str, age: int):
        print(f'name: {name} and age: {age}')

    name = get_name()
    age = get_age()
    greet(name, age)

dag_instance = hellow_world_etl()
