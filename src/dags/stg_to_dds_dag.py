import time
import json

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

postgres_conn_id = 'postgresql_de'

args = {
    "owner": "student",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

with DAG(
        'stg_to_dds',
        default_args=args,
        description='sprint5_project',
        catchup=True,
        start_date=datetime.today(),
        max_active_runs = 1,
) as dag:
    couriers = PostgresOperator(
        task_id='insert_into_dm_couriers',
        postgres_conn_id=postgres_conn_id,
        sql="insert into dds.dm_couriers (courier_id, courier_name) select distinct on (_id) * from stg.apisystem_couriers;"
    )
    timestamps = PostgresOperator(
        task_id='insert_into_dm_timestamps',
        postgres_conn_id=postgres_conn_id,
        sql="with t as (select distinct order_ts::timestamp as ts from stg.apisystem_deliveries) insert into dds.dm_timestamps (ts, year, month, day, time, date) select t.ts, extract(year from t.ts), extract(month from t.ts), extract(day from t.ts), t.ts::time, t.ts::date from t;"
    )
    deliveries = PostgresOperator(
        task_id='insert_into_dm_deliveries',
        postgres_conn_id=postgres_conn_id,
        sql="with a as (select order_id, order_ts::timestamp, courier_id, cast(rate as int), cast(sum as numeric(14,2)), cast(tip_sum as numeric(14,2)) from stg.apisystem_deliveries) insert into dds.dm_deliveries (order_id, courier_id, time_id, rate, sum, tip) select a.order_id, c.id, t.id, a.rate, a.sum, a.tip_sum from a inner join (select id, courier_id from dds.dm_couriers) c on a.courier_id = c.courier_id inner join (select id, ts from dds.dm_timestamps dt) t on a.order_ts = t.ts;"
    )
    (
        [couriers, timestamps]
        >> deliveries
    )