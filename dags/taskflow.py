from airflow.decorators import dag, task 
# from airflow.operators.python import PythonOperator 
from datetime import datetime 

@dag(
    start_date=datetime(2025,1,1),
    schedule_interval="@daily",
    catchup=False,
    tags=['task_flow']
)  

def taskflow():
    @task
    def task_a():
        print("Task A")
        return 68
    
    @task
    def task_b(value):
        print("Task B")
        print(value)
    
    task_b(task_a())
    
taskflow()
    