from airflow.decorators import dag, task
from datetime import datetime, timedelta
# from airflow.operators.python import PythonOperator
import random
 
@dag(
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    description='A simple DAG to generate and check random numbers',
    catchup=False
)
def homework1(): 

    #With using TaskFlow I don't need to define any xcom to transfer data between these tasks 
    @task
    def generate_random_number():
        number = random.randint(1, 100)
        # ti.xcom_push(key='random_number', value=number)
        
        print(f"Generated random number: {number}")
        return number

    @task
    def check_even_odd(randome_num):
        # number = ti.xcom_pull(task_ids='generate_random_number', key='random_number')
        result = "even" if randome_num % 2 == 0 else "odd"
        print(f"The number {randome_num} is {result}.")
        
        
    result_task1 = generate_random_number()
    check_even_odd(result_task1)
         
homework1()