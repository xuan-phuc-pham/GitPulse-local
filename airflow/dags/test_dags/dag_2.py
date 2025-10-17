from datetime import datetime, timedelta
from airflow import DAG # import from airflow
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'xpham',
    'retries': 5,
    'retry_delay': timedelta(minutes=3)
    
}

def hlw(ti):
    xcom1 = ti.xcom_pull(task_ids='task_2_py', key='key1')
    xcom2 = ti.xcom_pull(task_ids='task_2_py', key='key2')
    print(xcom1)
    print(xcom2)

def get_number(ti):
    ti.xcom_push(key='key1', value="This is xcom key 1")
    ti.xcom_push(key='key2', value="This is xcom key 2")


with DAG(
    # Some value as a param
    dag_id='dag_2',
    default_args=default_args, # 
    description='this is the test dag',
    start_date=datetime(2025, 9, 15, 10),
    schedule='@daily',
) as dag:
    task1 = PythonOperator(
        task_id='task_1_py',
        python_callable=hlw,
    )

    task2 = PythonOperator(
        task_id='task_2_py',
        python_callable=get_number,
    )

    task2 >> task1