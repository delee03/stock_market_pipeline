from airflow.decorators import dag, task
from datetime import datetime


@dag(schedule_interval='@daily', start_date=datetime(2021, 1, 1), catchup=False)
def stocker_market():
    @task()
    def fetch_stock_data():
        print('Fetching stock data')

    @task()
    def process_stock_data():
        print('Processing stock data')

    fetch_stock_data >> process_stock_data
