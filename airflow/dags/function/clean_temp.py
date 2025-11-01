from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta

def clean_temp_files(**context):
    s3_conn = S3Hook(aws_conn_id='minio_conn')
    date = (context['logical_date'] - timedelta(days=1)).strftime('%Y-%m-%d')
    for hour in range(24):
        s3_conn.delete_objects(bucket='airflow', keys=[f'gh_data/events/{date}/events_{date}_{hour}.csv'])
        s3_conn.delete_objects(bucket='airflow', keys=[f'gh_data/users/{date}/users_{date}_{hour}.csv'])
        s3_conn.delete_objects(bucket='airflow', keys=[f'gh_data/repos/{date}/repos_{date}_{hour}.csv'])
        s3_conn.delete_objects(bucket='airflow', keys=[f'gh_data/orgs/{date}/orgs_{date}_{hour}.csv'])

        
def clean_temp_parquet(**context):
    s3_conn = S3Hook(aws_conn_id='minio_conn')
    date = (context['logical_date'] - timedelta(days=1)).strftime('%Y-%m-%d')

    prefixes = [
        f"gh_data_staging/events/{date}/",
        f"gh_data_staging/users/{date}/",
        f"gh_data_staging/repos/{date}/",
        f"gh_data_staging/orgs/{date}/"
    ]

    for prefix in prefixes:
        keys = s3_conn.list_keys(bucket_name='airflow', prefix=prefix)
        if keys:
            s3_conn.delete_objects(bucket='airflow', keys=keys)
            print(f"✅ Deleted {len(keys)} objects under {prefix}")
        else:
            print(f"⚠️ No objects found under {prefix}")