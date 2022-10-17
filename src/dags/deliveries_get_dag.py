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

d_url = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries'

def upload(url, headers,conn):
    response= requests.get(url, headers=headers)
    temp = response.content
    temp = json.loads(temp)
    df = pd.DataFrame(temp)
    pg_hook = PostgresHook(conn)
    pg_hook.insert_rows(table='stg.apisystem_deliveries', rows = df.values.tolist(), target_fields = ['oder_id', 'order_ts', 'delivery_id', 'courier_id', 'address', 'delivery_ts', 'rate', 'sum', 'tip_sum'])

args = {
    "owner": "student",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

business_dt = '{{ ds }}'

with DAG(
        'deliveries_get',
        default_args=args,
        description='sprint5_project',
        start_date=datetime.today(),
        catchup=True,
        max_active_runs = 1,
) as dag:
    deliveries_get = PythonOperator(
        task_id = 'get_deliveries_from_api',
        python_callable = upload,
        op_kwargs={'url': d_url,
                   'headers': headers,
                   'conn': postgres_conn_id}
    )
    (
        deliveries_get   
    )