from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'xpham',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='test_dag_cron', 
    default_args=default_args,
    start_date=datetime(2025, 8, 1),
    schedule='0 6 * * *'
) as dag:
    t1 = BashOperator(
        task_id='print_date',
        bash_command='date'
    )

    t2 = BashOperator(
        task_id='sleep',
        bash_command='sleep 5'
    )

    t1 >> t2

