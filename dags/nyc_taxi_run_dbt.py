import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from datasets import BRONZE_ASSET

default_args = {
    'owner': 'tuns',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2024, 1, 1),
}

DBT_PROJECT_PATH = os.environ.get('DBT_PROJECT_PATH', '/dbt_nyc_taxi')

with DAG(
    dag_id='nyc_taxi_run_dbt',
    default_args=default_args,
    schedule=[BRONZE_ASSET],
    catchup=False,
    tags=['dbt', 'transformation', 'silver', 'gold', 'nyc_taxi']
) as dag:

    dbt_seed = BashOperator(
        task_id='dbt_seed',
        bash_command=f'cd {DBT_PROJECT_PATH} && dbt seed'
    )

    dbt_snapshot = BashOperator(
        task_id='dbt_snapshot',
        bash_command=f'cd {DBT_PROJECT_PATH} && dbt snapshot'
    )
    
    dbt_run_silver = BashOperator(
        task_id='dbt_run_silver',
        bash_command=f'cd {DBT_PROJECT_PATH} && dbt run --select silver'
    )

    dbt_run_gold = BashOperator(
        task_id='dbt_run_gold',
        bash_command=f'cd {DBT_PROJECT_PATH} && dbt run --select gold'
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd {DBT_PROJECT_PATH} && dbt test'
    )

    dbt_seed >> dbt_snapshot >> dbt_run_silver >> dbt_run_gold >> dbt_test
