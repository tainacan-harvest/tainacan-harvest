""""""

from datetime import datetime, timedelta
from urllib.parse import urljoin
import os
import json
import pymongo

import requests
import pandas as pd

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# DAG
default_args = {
    'owner': 'nitai-tiago',
    'start_date': datetime(2021, 8, 12),
    'depends_on_past': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=20),
}

dag = DAG(
    'harvest_tainacan_endpoints',
    default_args=default_args,
    schedule_interval='33 2 * * *',
    catchup=False,
    description=__doc__,
    tags=['tainacan', 'stage']
)


def _execute_harvest():
    base_url = 'https://mhn.acervos.museus.gov.br/wp-json/tainacan/v2/'
    req_url = os.path.join(base_url, 'collections/')
    response = requests.get(req_url)
    collections = response.json()

    myclient = pymongo.MongoClient("mongodb://root:rootpassword@192.168.15.8:27017/")
    mydb = myclient["admin"]
    mycol = mydb["Tainacan-harvest"]
    def insert_db(item:dict):
        x = mycol.insert_one(item)
        print(x.inserted_id)

    for collection in collections:
        req_url = os.path.join(base_url, f'collection/{collection["id"]}/items')
        response = requests.get(req_url)
        items = response.json()['items']
        for item in items:
            insert_db(item)

# Tasks
# clear_db = PythonOperator(
#     task_id='clear_db',
#     python_callable=_clear_db,
#     dag=dag
# )

execute_harvest = PythonOperator(
    task_id='execute_harvest',
    python_callable=_execute_harvest,
    dag=dag
)

# clear_db >> execute_harvest