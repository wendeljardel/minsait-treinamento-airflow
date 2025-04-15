"""
"""

# import libraries
from airflow.decorators import dag, task
from datetime import datetime

# declare structure
@dag(
    dag_id="task-flow-dag",
    start_date=datetime(2024, 10, 29),
    catchup=False,
    is_paused_upon_creation=False,
)
def init():

    # tasks

    @task()
    def extract():
        return {"data": "extract"}
    
    @task()
    def transform(data: dict):
        return {"data": "transform"}

    transform(extract())

dag = init()

