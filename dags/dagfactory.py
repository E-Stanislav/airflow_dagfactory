import json
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

base_dir = "/opt/airflow"

dag_folder = os.path.join(base_dir, "dags", "dag_meta")
task_folder = os.path.join(base_dir, "dags", "task_meta")

list_dags = os.listdir(dag_folder)
# list_tasks = os.listdir(task_folder)


def read_meta(path: str) -> dict:
    with open(path, "r") as f:
        return json.loads(f.read())


def make_task(dag, task: str):
    task_meta = read_meta(f"{task_folder}/{task}.json")

    operator_type = task_meta["operator_type"]
    if operator_type == "DummyOperator":
        return DummyOperator(task_id=task_meta["task_name"], dag=dag)


def make_dag(dag_path: str):
    meta_dag = read_meta(f"{dag_folder}/{dag_path}")

    dag = DAG(
        dag_id=meta_dag["dag_id"],
        default_args=meta_dag["default_args"],
        description=meta_dag["description"],
        schedule_interval=meta_dag["schedule_interval"],
        owner_links={"airflow": "https://airflow.apache.org"},
    )
    tasks = meta_dag["tasks"]
    for task in tasks:
        make_task(dag=dag, task=task)

    return dag


for dag_path in list_dags:
    try:
        globals()[dag_path] = make_dag(dag_path=dag_path)
    except Exception as e:
        print(e)
