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

ENDPOINTS = {
    'MHN': 'https://mhn.acervos.museus.gov.br/',
    'ITAIPU': 'http://museudearqueologiadeitaipu.museus.gov.br/',
    'ARTE_SACRA': 'https://museudeartereligiosaetradicional.acervos.museus.gov.br/',
    'GOIAS': 'http://museusibramgoias.acervos.museus.gov.br/',
}

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
    concurrency=2,
    tags=['tainacan', 'stage']
)

def insert_db(item:dict):
    myclient = pymongo.MongoClient("mongodb://airflow:airflow@mongodb:27017/")
    mydb = myclient["admin"]
    mycol = mydb["Tainacan-harvest"]
    mycol.insert_one(item)

def _execute_harvest(endpoint: str):
    base_url = os.path.join(endpoint, 'wp-json/tainacan/v2/')
    get_coll_url = os.path.join(base_url, 'collections/')
    response = requests.get(get_coll_url)
    collections = response.json()

    for collection in collections:
        get_items_url = os.path.join(base_url, f'collection/{collection["id"]}/items')
        response = requests.get(get_items_url)
        items = response.json()['items']
        for item in items:
            insert_db(item)

# Tasks
# clear_db = PythonOperator(
#     task_id='clear_db',
#     python_callable=_clear_db,
#     dag=dag
# )

for site_name, endpoint in ENDPOINTS.items():
    execute_harvest = PythonOperator(
        task_id=f'execute_harvest_{site_name}',
        python_callable=_execute_harvest,
        op_kwargs={'endpoint': endpoint},
        dag=dag
    )

# clear_db >> execute_harvest