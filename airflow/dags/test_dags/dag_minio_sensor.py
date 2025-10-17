from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor


default_args = {
    'owner': 'xpham',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 28),
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='dag_minio_sensor_v4',
    default_args=default_args,
    description='A simple DAG with minio sensor',
    schedule='@daily',
    catchup=False,
) as dag:
    task1 = S3KeySensor(
        task_id='test_s3_key_sensor_v4',
        bucket_key='test_minio.csv',
        bucket_name='airflow',
        aws_conn_id='minio_conn',
        timeout=5*60,  # 5 minutes
        poke_interval=5
    )
    task2 = PythonOperator(
        task_id='task2_print_execution_date',
        python_callable=lambda ds, **kwargs: print(f"Execution date is {ds}"),
    )

    task1 >> task2


