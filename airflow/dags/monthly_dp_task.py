from airflow import DAG
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from airflow.operators.python import PythonOperator

from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'xpham',
    'depends_on_past': True,
    'start_date': datetime(2025, 10, 2 ),
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}

def dbt_monthly_run(**context):
    date = (context['logical_date'] - timedelta(days=1)).strftime('%Y-%m-%d')
    docker_task = DockerOperator(
        task_id='dbt_run',
        image='ghcr.io/dbt-labs/dbt-postgres:1.9.latest',  # your dbt container image
        working_dir='/usr/app/gh_pipeline',
        command=f"run --vars '{{\"logical_previous_day\":\"{date}\", \"\":\"{date}\" }}' -m tag:monthly",
        mounts=[
            Mount(source='/home/xpham/GitPulseLocal/dbt/usr/app', target='/usr/app', type='bind'),
            Mount(source='/home/xpham/GitPulseLocal/dbt', target='/root/.dbt', type='bind')
        ],
        network_mode='gitpulselocal_nw-1',
        docker_url='unix://var/run/docker.sock',
        auto_remove='success'
    )
    docker_task.execute(context=context)


def clean_dev(**context):
    hook = PostgresHook(postgres_conn_id='pg_conn')
    first_of_prev_month = (context['logical_date'].replace(day=1) - relativedelta(months=1)).strftime('%Y-%m-%d')
    query = f"DELETE FROM dev.raw_events WHERE created_at < TIMESTAMP '{first_of_prev_month}'"
    hook.run(query)

with DAG(
    dag_id='gitpulse_monthly_pipeline_v0',
    default_args=default_args,
    schedule="0 6 2 * *",  # Runs every month on 2th at 6:00
    catchup=True,
) as dag:
    monthly_dbt_task = PythonOperator(
        task_id='monthly_dbt_task',
        python_callable=dbt_monthly_run
    )
    clean_task = PythonOperator(
        task_id = 'clean_raw_area',
        python_callable=clean_dev
    )
    monthly_dbt_task >> clean_task