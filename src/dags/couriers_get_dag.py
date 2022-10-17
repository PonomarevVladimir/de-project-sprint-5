import time
import requests
import json
import pandas as pd

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.http_hook import HttpHook

postgres_conn_id = 'postgresql_de'

nickname = 'Vova'
cohort = '5'
api_key = '25c27781-8fde-4b30-a22e-524044a7580f'

headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-API-KEY': api_key
}

d_url = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers'

def upload(url, headers,conn):
    response= requests.get(url, headers=headers)
    temp = response.content
    temp = json.loads(temp)
    df = pd.DataFrame(temp)
    pg_hook = PostgresHook(conn)
    pg_hook.insert_rows(table='stg.apisystem_couriers', rows = df.values.tolist(), target_fields = ['_id', 'name'])

args = {
    "owner": "student",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

business_dt = '{{ ds }}'

with DAG(
        'couriers_get',
        default_args=args,
        description='sprint5_project',
        start_date=datetime.today(),
        catchup=True,
        max_active_runs = 1,
) as dag:
    couriers_get = PythonOperator(
        task_id = 'get_couriers_from_api',
        python_callable = upload,
        op_kwargs={'url': d_url,
                   'headers': headers,
                   'conn': postgres_conn_id}
    )
    (
        couriers_get   
    )