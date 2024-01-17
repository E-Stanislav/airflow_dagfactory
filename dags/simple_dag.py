from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def print_hello():
    print("Hello, Airflow!")

dag = DAG('simple_dag', description='Простой DAG', schedule_interval='@once', start_date=datetime(2022, 1, 1))

task = PythonOperator(
    task_id='print_hello_task',
    python_callable=print_hello,
    dag=dag
)

task
