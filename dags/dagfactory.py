import json
import os
import sys
import traceback

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.helpers import chain

base_dir = os.environ.get("AIRFLOW_HOME")
sys.path.append(base_dir)
from operators.customs_operators import PrintOperator, SleepOperator

dag_folder = os.path.join(base_dir, "dags", "dag_meta")
task_folder = os.path.join(base_dir, "dags", "task_meta")

list_dags = os.listdir(dag_folder)


def read_meta(path: str) -> dict:
    """Base func for reading file and convert to dict object

    Args:
        path (str): path to meta file

    Returns:
        dict: meta file
    """
    with open(path, "r") as f:
        return json.loads(f.read())


def make_task(dag: DAG, tasks: list) -> dict:
    """Making a dags tasks using the path to the meta file

    Args:
        dag (DAG): airflow dag object
        tasks (list): list tasks name

    Returns:
        dict: An object with the name of the operator task and dependent tasks
    """
    operators = {}

    for task in tasks:
        task_meta = read_meta(f"{task_folder}/{task}.json")

        operator_type = task_meta["operator_type"]
        depends_on = task_meta.get("depends_on", [])

        if operator_type == "DummyOperator":
            operators[task_meta["task_name"]] = {
                "dag_id": DummyOperator(
                    task_id=task_meta["task_name"], dag=dag
                ),
                "depends_on": depends_on,
            }

        elif operator_type == "PrintOperator":
            my_argument = task_meta["my_argument"]

            operators[task_meta["task_name"]] = {
                "dag_id": PrintOperator(
                    task_id=task_meta["task_name"],
                    my_argument=my_argument,
                    dag=dag,
                ),
                "depends_on": depends_on,
            }

        elif operator_type == "SleepOperator":
            time_sleep = task_meta["time_sleep"]

            operators[task_meta["task_name"]] = {
                "dag_id": SleepOperator(
                    task_id=task_meta["task_name"],
                    time_sleep=time_sleep,
                    dag=dag,
                ),
                "depends_on": depends_on,
            }
        else:
            raise TypeError("Нет такого оператора")

    return operators


def set_depends(operators: dict) -> None:
    """Setting dependencies between tasks in dags

    Args:
        operators (dict): An object with task name, operator and dependent tasks
    """
    try:
        for operator_name, operator_content in operators.items():
            if len(operator_content["depends_on"]) > 0:
                for depend_task in operator_content["depends_on"]:
                    chain(
                        operators[depend_task]["dag_id"],
                        operator_content["dag_id"],
                    )
    except Exception:
        traceback.print_exc()


def make_dag(dag_path: str) -> DAG:
    """Making a dag using the path to the meta file

    Args:
        dag_path (str): Path to the dag meta file

    Returns:
        DAG: airflow dag
    """

    meta_dag = read_meta(f"{dag_folder}/{dag_path}")

    dag = DAG(
        dag_id=meta_dag["dag_id"],
        default_args=meta_dag["default_args"],
        description=meta_dag["description"],
        schedule_interval=meta_dag["schedule_interval"],
        tags=meta_dag.get("tags"),
        catchup=meta_dag.get("catchup", False),
    )
    dag.doc_md = f"""Автогенерируемый даг [{meta_dag["dag_id"]}](https://airflow.apache.org/) \n
    
    Dag owner: {meta_dag["default_args"].get("owner", "airflow").capitalize()}"""
    tasks = meta_dag["tasks"]
    operators = make_task(dag=dag, tasks=tasks)
    set_depends(operators=operators)
    return dag


for dag_path in list_dags:
    try:
        globals()[dag_path] = make_dag(dag_path=dag_path)
    except Exception as e:
        print(e)
