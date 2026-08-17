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

def init_load():
    con = duckdb.connect('NYC_taxi.db')
    for f in os.listdir('/data'):
        con.read_parquet(f'/data/{f}').to_table(f[0:f.find('_')])


def incrementals_load():
    try:
        con = duckdb.connect('NYC_taxi.db')
        for f in os.listdir('/data'):
            t = con.read_parquet(f'/data/{f}')
            con.execute(f"insert into {f[0:f.find('_')]} select * from t")

        print('Load data successfully')

    except Exception as e:
        print('We got errors!:\n', e)

def printsuc():
    print('success')

with DAG(
    dag_id = 'Ingestion2',
    default_args = default_args,
    schedule = '@monthly',
    catchup = False
) as dag:
    task1 = PythonOperator(task_id = 'Initial_load', python_callable = init_load)
    task2 = PythonOperator(task_id = 'Incre_load', python_callable = incrementals_load)
    
    task1 >> task2





