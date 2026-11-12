import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

from datasets import ADLS_RAW_ASSET, BRONZE_ASSET

default_args = {
    'owner': 'tuns',
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2024, 1, 1),
}

# Fetch Databricks Job ID from Airflow Variable or Environment Variable
DATABRICKS_CONN_ID = os.environ.get('DATABRICKS_CONN_ID', 'databricks_default')
DATABRICKS_JOB_ID = os.environ.get('DATABRICKS_BRONZE_JOB_ID') or Variable.get('DATABRICKS_BRONZE_JOB_ID', default_var='123456789')

with DAG(
    dag_id='nyc_taxi_adls_to_bronze',
    default_args=default_args,
    schedule=[ADLS_RAW_ASSET],
    catchup=False,
    tags=['databricks', 'bronze', 'autoloader', 'nyc_taxi']
) as dag:

    trigger_bronze_autoloader = DatabricksRunNowOperator(
        task_id='run_bronze_ingestion',
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=DATABRICKS_JOB_ID,
        outlets=[BRONZE_ASSET]
    )

    trigger_bronze_autoloader
