import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def list_parquet_files_from_minio(minio_conn_id, bucket_name, prefix):
    hook = S3Hook(aws_conn_id=minio_conn_id)
    keys = hook.list_keys(bucket_name=bucket_name, prefix=prefix)
    parquet_files = [k for k in keys if k.endswith(".parquet")]
    return parquet_files

def create_temp_tables(hook):
    hook.run( """
    CREATE SCHEMA IF NOT EXISTS temp;
    CREATE TABLE IF NOT EXISTS temp.raw_temp_users (
        id BIGINT,
        login VARCHAR NOT NULL,
        display_login VARCHAR,
        gravatar_id VARCHAR,
        url VARCHAR NOT NULL,
        avatar_url VARCHAR
    );
    CREATE TABLE IF NOT EXISTS temp.raw_temp_orgs (
        id BIGINT,
        login VARCHAR NOT NULL,
        gravatar_id VARCHAR,
        url VARCHAR NOT NULL,
        avatar_url VARCHAR
    );
    CREATE TABLE IF NOT EXISTS temp.raw_temp_repos (
        id BIGINT,
        name VARCHAR NOT NULL,
        url VARCHAR NOT NULL
    );
    """)

def check_and_create_tables(hook):
    hook.run("""
    CREATE SCHEMA IF NOT EXISTS dev;
    CREATE TABLE IF NOT EXISTS dev.raw_events (
        id BIGINT PRIMARY KEY,
        type VARCHAR NOT NULL,
        actor_id BIGINT NOT NULL,
        repo_id BIGINT NOT NULL,
        public BOOLEAN NOT NULL,
        created_at TIMESTAMP NOT NULL,
        org_id BIGINT
    );
    CREATE INDEX IF NOT EXISTS idx_create_time ON dev.raw_events(created_at);
    CREATE TABLE IF NOT EXISTS dev.raw_users (
        id BIGINT PRIMARY KEY,
        login VARCHAR NOT NULL,
        display_login VARCHAR,
        gravatar_id VARCHAR,
        url VARCHAR NOT NULL,
        avatar_url VARCHAR
    );
    CREATE TABLE IF NOT EXISTS dev.raw_repos (
        id BIGINT PRIMARY KEY,
        name VARCHAR NOT NULL,
        url VARCHAR NOT NULL
    );
    CREATE TABLE IF NOT EXISTS dev.raw_orgs (
        id BIGINT PRIMARY KEY,
        login VARCHAR NOT NULL,
        gravatar_id VARCHAR,
        url VARCHAR NOT NULL,
        avatar_url VARCHAR
    );
    """)


def import_s3_pg(date, minio_conn, hook):
    engine = hook.get_sqlalchemy_engine()
    key = minio_conn.extra_dejson.get("aws_access_key_id")
    secret = minio_conn.extra_dejson.get("aws_secret_access_key")
    endpoint_url = minio_conn.extra_dejson.get("endpoint_url")

    print("Connection:",[key, secret, endpoint_url])

    # The root directory of your Parquet dataset

    spark_s3_prefix = "s3://"
    bucket_name = "airflow"
    prefix_dir = "gh_data_staging"
    
    events_parquet_files_names = list_parquet_files_from_minio(minio_conn_id = "minio_conn", bucket_name = bucket_name, prefix=f"{prefix_dir}/events/{date}")
    users_parquet_files_names = list_parquet_files_from_minio(minio_conn_id = "minio_conn", bucket_name = bucket_name, prefix=f"{prefix_dir}/users/{date}")
    repos_parquet_files_names = list_parquet_files_from_minio(minio_conn_id = "minio_conn", bucket_name = bucket_name, prefix=f"{prefix_dir}/repos/{date}")
    orgs_parquet_files_names = list_parquet_files_from_minio(minio_conn_id = "minio_conn", bucket_name = bucket_name, prefix=f"{prefix_dir}/orgs/{date}")

    #test
    print(events_parquet_files_names)

    hook.run(
        f"""
            DELETE FROM dev.raw_events
            WHERE DATE(created_at) = '{date}';
        """
    )

    for filename in events_parquet_files_names:
        event_parquet_path = f"{spark_s3_prefix}{bucket_name}/{filename}"
        event_df = pd.read_parquet(
            event_parquet_path,
            engine="pyarrow",
            storage_options={
                "key": key,
                "secret": secret,
                "client_kwargs": {
                    "endpoint_url": endpoint_url
                }
            }
        )
        event_df.to_sql('raw_events', engine, schema='dev', if_exists='append', index=False)
        print("inserted", filename)
        del event_df

    for filename in users_parquet_files_names:
        users_parquet_path = f"{spark_s3_prefix}{bucket_name}/{filename}"
        users_df = pd.read_parquet(
            users_parquet_path,
            engine="pyarrow",
            storage_options={
                "key": key,
                "secret": secret,
                "client_kwargs": {
                    "endpoint_url": endpoint_url
                }
            }
        )
        users_df.to_sql('raw_temp_users', engine, schema='temp', if_exists='append', index=False)
        print("inserted", filename)
        del users_df

    for filename in repos_parquet_files_names:
        repos_parquet_path = f"{spark_s3_prefix}{bucket_name}/{filename}"
        repos_df = pd.read_parquet(
            repos_parquet_path,
            engine="pyarrow",
            storage_options={
                "key": key,
                "secret": secret,
                "client_kwargs": {
                    "endpoint_url": endpoint_url
                }
            }
        )
        repos_df.to_sql('raw_temp_repos', engine, schema='temp', if_exists='append', index=False)
        print("inserted", filename)
        del repos_df

    for filename in orgs_parquet_files_names:
        orgs_parquet_path = f"{spark_s3_prefix}{bucket_name}/{filename}"
        orgs_df = pd.read_parquet(
            orgs_parquet_path,
            engine="pyarrow",
            storage_options={
                "key": key,
                "secret": secret,
                "client_kwargs": {
                    "endpoint_url": endpoint_url
                }
            }
        )
        orgs_df.to_sql('raw_temp_orgs', engine, schema='temp', if_exists='append', index=False)
        print("inserted", filename)
        del orgs_df
    
    hook.run(
        """
            INSERT INTO dev.raw_users (id, login, display_login, gravatar_id, url, avatar_url)
            SELECT id, login, display_login, gravatar_id, url, avatar_url
            FROM temp.raw_temp_users
            ON CONFLICT (id) DO UPDATE SET
                login = EXCLUDED.login,
                display_login = EXCLUDED.display_login,
                gravatar_id = EXCLUDED.gravatar_id,
                url = EXCLUDED.url,
                avatar_url = EXCLUDED.avatar_url;
            DROP TABLE temp.raw_temp_users;
        """
    )

    hook.run(
        """
            INSERT INTO dev.raw_orgs (id, login, gravatar_id, url, avatar_url)
            SELECT id, login, gravatar_id, url, avatar_url
            FROM temp.raw_temp_orgs
            ON CONFLICT (id) DO UPDATE SET
                login = EXCLUDED.login,
                gravatar_id = EXCLUDED.gravatar_id,
                url = EXCLUDED.url,
                avatar_url = EXCLUDED.avatar_url;
            DROP TABLE temp.raw_temp_orgs;
        """
    )

    hook.run(
        """
            INSERT INTO dev.raw_repos (id, name, url)
            SELECT id, name, url
            FROM temp.raw_temp_repos
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                url = EXCLUDED.url;
            DROP TABLE temp.raw_temp_repos;
        """
    )

def import_to_postgres(**context):
    # engine = create_engine('postgresql+psycopg2://postgres:postgres@db:5432/gh_archive')
    date = (context['logical_date'] - timedelta(days=1)).strftime('%Y-%m-%d')
    hook = PostgresHook(postgres_conn_id='pg_conn')
    check_and_create_tables(hook)
    create_temp_tables(hook)
    import_s3_pg(date, BaseHook.get_connection("minio_conn"), hook)


