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

PAGE_SIZE = 97 # Tamanho máximo 96 (não está documentado)

def get_collection_items(base_url: str, collection_id: int):
    items_url = os.path.join(base_url, f'collection/{collection_id}/items')
    all_items = []
    page = 0
    while True:
        params = [
            ('perpage', PAGE_SIZE),
            ('offset', page * PAGE_SIZE),
            ]
        response = requests.get(items_url, params=params)
        items = response.json()['items']
        if len(items) == 0:
            break
        all_items.extend(items)
        print(f'Baixou: {response.url}')
        page += 1

    return all_items

def _execute_harvest(endpoint: str):
    base_url = os.path.join(endpoint, 'wp-json/tainacan/v2/')
    get_coll_url = os.path.join(base_url, 'collections/')
    response = requests.get(get_coll_url)

    count_inserted = 0
    for collection in response.json():
        items = get_collection_items(base_url, collection['id'])
        for item in items:
            insert_db(item)
        count_inserted += len(items)

    print(f'TOTAL de items inseridos: {count_inserted}')

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