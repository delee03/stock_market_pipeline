from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator 
from airflow.sensors.base import PokeReturnValue
from datetime import datetime
import requests
from include.stock_market.tasks import _get_stock_prices, _store_prices
SYMBOL="nvda"

@dag(
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['stock_market'],

)
def stock_market():
    
    #With using sensor to wait for API available 
    @task.sensor(poke_interval=30, timeout=30, mode='poke')
    def is_api_available() -> PokeReturnValue:
        api = BaseHook.get_connection('stock_api')
        url = f"{api.host}{api.extra_dejson['endpoint']}"
        print(url)
        response = requests.get(url, headers=api.extra_dejson['headers'])
        condition = response.json()['finance']['result'] is None
        return PokeReturnValue(is_done=condition, xcom_value=url)
    
    getStockPrices = PythonOperator(
        task_id = "get_stock_prices",
        python_callable=_get_stock_prices,
        op_kwargs={'url': '{{ti.xcom_pull(task_ids="is_api_available")}}','symbol': SYMBOL}
    )
    
    store_prices = PythonOperator(
        task_id = 'store_prices', 
        python_callable= _store_prices,
        op_kwargs={'stock': '{{ti.xcom_pull(task_ids="get_stock_prices")}}'}
    )
    
    
    is_api_available() >> getStockPrices >> store_prices
        

stock_market()
    