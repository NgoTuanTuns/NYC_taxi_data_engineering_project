from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime, timedelta
from airflow.sdk import Asset

ADLS_RAW_ASSET = Asset("nyc_taxi/adls/raw")
BRONZE_ASSET   = Asset("nyc_taxi/databricks/bronze")
default_args = {
    'owner': 'tuns',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='nyc_taxi_adls_to_bronze',
    default_args=default_args,
    schedule=[ADLS_RAW_ASSET], 
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    task1 = DatabricksRunNowOperator(
        task_id='run_bronze_ingestion',
        databricks_conn_id='databricks_default',
        job_id='your_job_id',
        outlets=[BRONZE_ASSET]
    )