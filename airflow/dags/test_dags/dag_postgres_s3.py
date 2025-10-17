from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from tempfile import NamedTemporaryFile # the file will be cleaned up after closing


default_args = {
    'owner': 'xpham',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def task1_postgres_to_s3(**context):
    db_conn = PostgresHook(postgres_conn_id='db_conn')
    engine = db_conn.get_sqlalchemy_engine()
    sql = f"SELECT * FROM my_table WHERE DATE(created_at) ='{context['ds']}'"
    df = pd.read_sql(sql, engine)
    if not df.empty:
        print("There is data")
        print(df)
        with NamedTemporaryFile("w+", suffix=".csv", delete=True) as temp_file:
            s3_conn = S3Hook(aws_conn_id='minio_conn')  
            df.to_csv(temp_file.name, index=False)
            temp_file.flush()  # Ensure data is written to disk
            s3_conn.load_file(
                filename=temp_file.name,
                key=f"/airflow_test/data_{context['ds']}.csv",
                bucket_name='airflow',
                replace=True
            )

with DAG(
    dag_id='dag_postgres_to_s3',
    default_args=default_args,
    description='A simple DAG with sensors',
    schedule='@daily',
    catchup=True,
) as dag:
    task1 = PythonOperator(
        task_id='task1_postgres_to_s3',
        python_callable=task1_postgres_to_s3,
        depends_on_past=True,   
    )
    task1
