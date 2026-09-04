from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator

DBT_PROJECT_PATH = '/dbt_nyc_taxi'

default_args = {
    'owner': 'tuns',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='nyc_taxi_run_dbt',
    default_args=default_args,
    schedule='@monthly',
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:
    silver = BashOperator(
        task_id = 'dbt_run_silver',
        bash_command = f'cd {DBT_PROJECT_PATH} && dbt run --select silver'
    )

    gold = BashOperator(
        task_id = 'dbt_run_gold',
        bash_command = f'cd {DBT_PROJECT_PATH} && dbt run --select gold'
    )

    silver >> gold

