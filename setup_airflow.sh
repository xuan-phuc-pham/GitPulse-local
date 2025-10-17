#!/bin/bash

docker compose exec af bash -c "cp /opt/airflow/simple_auth_manager_passwords.json.generated /opt/airflow/data"

docker compose exec af bash -c "airflow connections add spark_conn --conn-type spark --conn-host spark://spark-master --conn-port 7077"

docker compose exec af bash -c "airflow connections add minio_conn \
--conn-type aws \
--conn-extra '{\"aws_access_key_id\": \"Zqa93aDnKgFCP2SwJGA2\", \"aws_secret_access_key\": \"O2PRXhHxAOn35unRwUXpVBrFzVApSjglkk5jT9Kv\", \"endpoint_url\": \"http://minio:9000\"}'"

docker compose exec af bash -c "airflow connections add 'pg_conn' \
    --conn-type postgres \
    --conn-host db \
    --conn-schema gh_archive \
    --conn-login postgres \
    --conn-password postgres \
    --conn-port 5432"