import duckdb
import os
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

default_args = {
    'owner':'Tuns',
    'retries':5,
    'retry_delay':timedelta(minutes=2)
}

def incrementals_load():
    try:
        con = duckdb.connect('NYC_taxi.db')
        for f in os.listdir('data'):
            t = con.read_parquet(f'data/{f}')
            con.execute(f"insert into {f[0:f.find('_')]} select * from t")

    except Exception as e:
        print('We got errors!:\n', e)


with DAG(
    dag_id = 'Ingestion',
    default_args = default_args,
    start_date = datetime(2026, 9, 1),
    schedule = '@monthly',
    catchup = False
) as dag:
    task1 = PythonOperator(task_id = 'Incre_load', python_callable = incrementals_load)





