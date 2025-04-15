"""
"""

# TODO 1: import libraries
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# TODO 2: declare DAG structure
with DAG(
    dag_id="traditional-dag",
    start_date=datetime(2024, 10, 29),
    catchup=False
) as dag:
    
    # TODO 3: declare functions

    # TODO function 1: extract data
    def extract():
        return {"data": "extract"}

    # TODO function 2: transform data
    def transform(ti):
        data = ti.xcom_pull(task_ids="extract")
        return {"data": "transform"}

    # TODO declare task [1]
    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=extract
    )

    # TODO declare task [2]
    transform_data = PythonOperator(
        task_id="transform_data",
        python_callable=transform
    )

    # TODO 4: set task dependencies
    extract_data >> transform_data

    




