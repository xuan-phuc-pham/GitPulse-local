import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

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
    hook.run( """
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


def read_partitioned_parquet_from_minio(date, minio_conn, hook):
    engine = hook.get_sqlalchemy_engine()
    key = minio_conn.extra_dejson.get("aws_access_key_id")
    secret = minio_conn.extra_dejson.get("aws_secret_access_key")
    endpoint_url = minio_conn.extra_dejson.get("endpoint_url")

    print("Connection:",[key, secret, endpoint_url])

    # The root directory of your Parquet dataset
    event_path = f"s3://airflow/gh_data_staging/events/{date}/"
    user_path = f"s3://airflow/gh_data_staging/users/{date}/"
    org_path = f"s3://airflow/gh_data_staging/orgs/{date}/"
    repo_path = f"s3://airflow/gh_data_staging/repos/{date}/"

    # pandas will recursively read all partitions

    event_df = pd.read_parquet(
        event_path,
        engine="pyarrow",
        storage_options={
            "key": key,
            "secret": secret,
            "client_kwargs": {
                "endpoint_url": endpoint_url
            }
        }
    )

    user_df = pd.read_parquet(
        user_path,
        engine="pyarrow",
        storage_options={
            "key": key,
            "secret": secret,
            "client_kwargs": {
                "endpoint_url": endpoint_url
            }
        }
    )

    org_df = pd.read_parquet(
        org_path,
        engine="pyarrow",
        storage_options={
            "key": key,
            "secret": secret,
            "client_kwargs": {
                "endpoint_url": endpoint_url
            }
        }
    )

    repo_df = pd.read_parquet(
        repo_path,
        engine="pyarrow",
        storage_options={
            "key": key,
            "secret": secret,
            "client_kwargs": {
                "endpoint_url": endpoint_url
            }
        }
    )
    print("DataFrames read from MinIO:")
    # print("Events DataFrame:", event_df.head())
    # print("Users DataFrame:", user_df.head())
    # print("Orgs DataFrame:", org_df.head())
    # print("Repos DataFrame:", repo_df.head())
    event_df.to_sql('raw_events', engine, schema='dev', if_exists='append', index=False)

    user_df.to_sql('raw_temp_users', engine, schema='temp', if_exists='replace', index=False)
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
    org_df.to_sql('raw_temp_orgs', engine, schema='temp', if_exists='replace', index=False)
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
    repo_df.to_sql('raw_temp_repos', engine, schema='temp', if_exists='replace', index=False)
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
    read_partitioned_parquet_from_minio(date, BaseHook.get_connection("minio_conn"), hook)


