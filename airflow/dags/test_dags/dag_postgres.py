from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from tempfile import NamedTemporaryFile

default_args = {
    'owner': 'xpham',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def create_table(**context):
    ds = context["ds"]  # execution date
    hook = PostgresHook(postgres_conn_id='db_conn')
    hook.run("""
        CREATE TABLE IF NOT EXISTS my_table (
            id SERIAL PRIMARY KEY,
            dat_str VARCHAR(50),
            created_at TIMESTAMP
        );
    """)

def insert_data(**context):
    ds = context["ds"]  # execution date
    hook = PostgresHook(postgres_conn_id='db_conn')
    hook.run(f"""
        INSERT INTO my_table (dat_str, created_at) VALUES ('Sample Data generated on Airflow1', '{ds}');
        INSERT INTO my_table (dat_str, created_at) VALUES ('Sample Data generated on Airflow2', '{ds}');
    """)

with DAG(
    dag_id='postgres_dag_v1',
    default_args=default_args,
    start_date=datetime(2025, 9, 10),
    schedule='@daily',
    catchup=True
) as dag:
    create_table = PythonOperator(
        task_id='create_table',
        python_callable=create_table,
        depends_on_past=True,
    )

    insert_data = PythonOperator(
        task_id='insert_data',
        python_callable=insert_data,
        depends_on_past=True,
    )

    create_table >> insert_data


