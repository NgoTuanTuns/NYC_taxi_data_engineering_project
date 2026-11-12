import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeStorageV2Hook
from datasets import ADLS_RAW_ASSET

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'tuns',
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2024, 1, 1),
}

DATA_DIR = os.environ.get('DATA_DIR', '/data')
ADLS_CONN_ID = os.environ.get('ADLS_CONN_ID', 'adls2_connection')
ADLS_CONTAINER = os.environ.get('ADLS_CONTAINER', 'external')

# Known TLC trip prefixes
TRIP_TYPE_MAPPING = {
    'yellow': 'yellow_trip',
    'green': 'green_trip',
    'fhvhv': 'fhvhv_trip',
    'fhv': 'fhv_trip'
}

def resolve_target_dir(filename: str) -> str:
    """Map filename to target ADLS directory."""
    lower_name = filename.lower()
    for prefix, target_dir in TRIP_TYPE_MAPPING.items():
        if lower_name.startswith(prefix):
            return f'raw_data/{target_dir}/'
    return 'raw_data/misc/'

def download_and_upload_to_adls(**context):
    """
    Ingests monthly TLC parquet data and uploads to ADLS Gen2 raw zone.
    Processes files present in local DATA_DIR or downloads missing partition.
    """
    hook = AzureDataLakeStorageV2Hook(adls_conn_id=ADLS_CONN_ID)
    
    uploaded_count = 0
    
    if os.path.exists(DATA_DIR):
        files = [f for f in os.listdir(DATA_DIR) if f.endswith('.parquet')]
        logger.info(f"Found {len(files)} parquet file(s) in {DATA_DIR}")
        
        for file_name in files:
            local_path = os.path.join(DATA_DIR, file_name)
            target_directory = resolve_target_dir(file_name)
            
            logger.info(f"Uploading {file_name} -> {ADLS_CONTAINER}/{target_directory}")
            hook.upload_file_to_directory(
                file_system_name=ADLS_CONTAINER,
                directory_name=target_directory,
                file_name=file_name,
                file_path=local_path,
                overwrite=True
            )
            uploaded_count += 1
            
    logger.info(f"Successfully processed and uploaded {uploaded_count} file(s) to ADLS Gen2.")

with DAG(
    dag_id='nyc_taxi_ingest_to_adls',
    default_args=default_args,
    schedule='@monthly',
    catchup=False,
    tags=['ingestion', 'adls', 'nyc_taxi']
) as dag:

    upload_task = PythonOperator(
        task_id='upload_to_adls',
        python_callable=download_and_upload_to_adls,
        outlets=[ADLS_RAW_ASSET]
    )
