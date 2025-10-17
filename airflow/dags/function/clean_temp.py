from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta

def clean_temp_files(**context):
    s3_conn = S3Hook(aws_conn_id='minio_conn')
    date = (context['logical_date'] - timedelta(days=1)).strftime('%Y-%m-%d')
    for hour in range(24):
        s3_conn.delete_objects(bucket='airflow', keys=[f'gh_data/events/events_{date}_{hour}.csv'])
        s3_conn.delete_objects(bucket='airflow', keys=[f'gh_data/users/users_{date}_{hour}.csv'])
        s3_conn.delete_objects(bucket='airflow', keys=[f'gh_data/repos/repos_{date}_{hour}.csv'])
        s3_conn.delete_objects(bucket='airflow', keys=[f'gh_data/orgs/orgs_{date}_{hour}.csv'])

        
    