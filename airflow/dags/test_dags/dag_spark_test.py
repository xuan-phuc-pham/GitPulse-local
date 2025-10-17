from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator   


default_args = {
    'owner': 'xpham',
    'depends_on_past': True,
    'start_date': datetime(2025, 10, 10),
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}


with DAG(
    dag_id='test_spark_job',
    default_args=default_args,
    schedule="0 6 * * *",  # Runs every day at 06:00
    catchup=False,
) as dag:

    python_job = SparkSubmitOperator(
        task_id='spark_job',
        conn_id='spark_conn',
        application='/opt/shared/spark_airflow/test.py',
        dag=dag,
        verbose=1,
    )

    python_job


