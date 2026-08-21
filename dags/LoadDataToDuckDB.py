import os
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.sdk import Variable
from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeStorageV2Hook

default_args = {
    'owner':'Tuns',
    'retries':5,
    'retry_delay':timedelta(minutes=2)
}

def upload_data():
    try:
        hook = AzureDataLakeStorageV2Hook(adls_conn_id = 'adls2_connection')
        print(os.listdir('/data'))
        for i in os.listdir('/data'):
            print(i)
            hook.upload_file_to_directory(
                file_system_name = 'nyctaxidata',
                directory_name = 'raw_data',
                file_name = f'{i[0:i.find('_', i.find('_') + 1, len(i))]}.parquet',
                file_path = f'/data/{i}',
                overwrite = True
            )
    except Exception as e:
        print('We got errors!: \n', e)

with DAG(
    dag_id = 'Ingestion',
    default_args = default_args,
    schedule = '@monthly',
    catchup = False
) as dag:
    task2 = PythonOperator(task_id = 'upload_file', python_callable = upload_data)
    
    task2





