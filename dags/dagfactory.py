import json
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

base_dir = "/opt/airflow"

dag_folder = os.path.join(base_dir, "dags", "dag_meta")
task_folder = os.path.join(base_dir, "dags", "task_meta")

list_dags = os.listdir(dag_folder)


def read_meta(path: str) -> dict:
    with open(path, "r") as f:
        return json.loads(f.read())


def make_task(dag, tasks: list):
    # Dictionary for storing operators
    operators = {}

    for task in tasks:
        task_meta = read_meta(f"{task_folder}/{task}.json")

        operator_type = task_meta["operator_type"]
        depends_on = task_meta.get("depends_on")

        if operator_type == "DummyOperator":
            operators[task_meta["task_name"]] = {
                "dag_id": DummyOperator(
                    task_id=task_meta["task_name"], dag=dag
                ),
                "depends_on": depends_on,
            }

    return operators


def set_depends(operators: dict) -> None:
    
    for operator_name, operator_content in operators.items():
        if not operator_content["depends_on"] is None:
            for depend_task in operator_content["depends_on"]:
                operators[depend_task]["dag_id"] >> operator_content["dag_id"]

            
            



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
    operators = make_task(dag=dag, tasks=tasks)
    set_depends(operators=operators)

    return dag


for dag_path in list_dags:
    try:
        globals()[dag_path] = make_dag(dag_path=dag_path)
    except Exception as e:
        print(e)
