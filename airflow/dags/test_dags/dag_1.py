from datetime import datetime, timedelta
from airflow import DAG # import from airflow
from airflow.operators.bash import BashOperator

default_args ={
    'owner': 'xpham',
    'retries': 5,
    'retry_delay': timedelta(minutes=3)
} # dict used as default_args

with DAG(
    # Some value as a param
    dag_id='dag_1',
    default_args=default_args, # 
    description='this is the test dag',
    start_date=datetime(2025, 9, 15, 10),
    schedule='@daily',
) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command='for i in {1..10}; do echo $i; sleep 1; done; echo congrats t1'
    )
    task2 = BashOperator(
        task_id='second_task',
        bash_command='for i in {10..1}; do echo $i; sleep 1; done; echo congrats t2' 
    )
    task3 = BashOperator(
        task_id='third_task',
        bash_command='for i in {1..10}; do echo $i; sleep 1; done; echo congrats t3'
    )
    task1 >> task2
    task1 >> task3
    