from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.sdk import Asset
DBT_PROJECT_PATH = '/dbt_nyc_taxi'
BRONZE_ASSET     = Asset("nyc_taxi/databricks/bronze")
default_args = {
    'owner': 'tuns',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='nyc_taxi_run_dbt',
    default_args=default_args,
    schedule=[BRONZE_ASSET],
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:
    dbt_seed = BashOperator(
        task_id = 'dbt_seed',
        bash_command = f'cd {DBT_PROJECT_PATH} && dbt seed'
    )

    dbt_snapshot = BashOperator(
        task_id = 'dbt_snapshot',
        bash_command = f'cd {DBT_PROJECT_PATH} && dbt snapshot'
    )
    
    silver = BashOperator(
        task_id = 'dbt_run_silver',
        bash_command = f'cd {DBT_PROJECT_PATH} && dbt run --select silver'
    )

    gold = BashOperator(
        task_id = 'dbt_run_gold',
        bash_command = f'cd {DBT_PROJECT_PATH} && dbt run --select gold'
    )

    dbt_test = BashOperator(
        task_id = 'dbt_test',
        bash_command = f'cd {DBT_PROJECT_PATH} && dbt test'
    )

    dbt_seed >> dbt_snapshot >> silver >> gold >> dbt_test

