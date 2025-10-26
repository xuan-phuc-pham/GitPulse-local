from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from function.ingest_json_to_csv import ingest_json_to_csv
from function.clean_temp import *
from function.import_to_pg import import_to_postgres

default_args = {
    'owner': 'xpham',
    'depends_on_past': True,
    'start_date': datetime(2025, 10, 18),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def staging(**context):
    date = (context['logical_date'] - timedelta(days=1)).strftime('%Y-%m-%d')
    spark_task = SparkSubmitOperator(
        task_id='spark_job',
        conn_id='spark_conn',
        application='/opt/shared/spark_airflow/staging.py',
        dag=dag,
        application_args=['--target_date',  date],
        verbose=1,
    )
    spark_task.execute(context=context)

def dbt_daily_run(**context):
    date = (context['logical_date'] - timedelta(days=1)).strftime('%Y-%m-%d')
    docker_task = DockerOperator(
        task_id='dbt_run',
        image='ghcr.io/dbt-labs/dbt-postgres:1.9.latest',  # your dbt container image
        working_dir='/usr/app/gh_pipeline',
        command=f"run --vars '{{\"logical_previous_day\":\"{date}\"}}' -m tag:daily",
        mounts=[
            Mount(source='/home/xpham/dp_gh/dbt/usr/app', target='/usr/app', type='bind'),
            Mount(source='/home/xpham/dp_gh/dbt', target='/root/.dbt', type='bind')
        ],
        network_mode='dp_gh_nw-1',
        docker_url='unix://var/run/docker.sock',
        auto_remove='success'
    )
    docker_task.execute(context=context)

with DAG(
    dag_id='gh_archive_pipeline_v1',
    default_args=default_args,
    schedule="0 6 * * *",  # Runs every day at 6:00
    catchup=False,
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_json_to_csv',
        python_callable=ingest_json_to_csv,
    )

    staging_task = PythonOperator(
        task_id="dynamic_spark_submit",
        python_callable=staging,
    )

    clean_temp_task = PythonOperator(
        task_id='clean_temp_files',
        python_callable=clean_temp_files,
    )

    import_pg_task = PythonOperator(
        task_id='import_to_postgres',
        python_callable=import_to_postgres
    )

    dbt_daily_task = PythonOperator(
        task_id='dbt_daily_run',
        python_callable=dbt_daily_run
    )

    clean_parquet_task = PythonOperator(
        task_id='clean_temp_parquet',
        python_callable=clean_temp_parquet
    )


    ingest_task >> staging_task >> clean_temp_task
    staging_task >> import_pg_task >>  dbt_daily_task
    import_pg_task >> clean_parquet_task