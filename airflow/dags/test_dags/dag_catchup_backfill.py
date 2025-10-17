from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'xpham',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='dag_catchup_backfill_v3',
    default_args=default_args,
    start_date=datetime(2025, 9, 10),
    schedule='@daily',
    catchup=False  
) as dag:
    task1 = BashOperator(
        task_id='task_1_bash',
        bash_command='echo "Bash Operator Task Complete"',
    )

    task1



