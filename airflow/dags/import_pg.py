from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from function.import_to_pg import import_to_postgres

default_args = {
    'owner': 'xpham',
    'depends_on_past': True,
    'start_date': datetime(2025, 9, 1),
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}

with DAG(                                      
    dag_id='import_to_pg_v1',
    default_args=default_args,
    schedule="0 6 * * *",  # Runs every day at 06:00
    catchup=False,
) as dag:
    import_pg = PythonOperator(
        task_id='import_to_postgres',
        python_callable=import_to_postgres
    )
    import_pg


