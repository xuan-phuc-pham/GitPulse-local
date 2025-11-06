#!/bin/bash

docker compose exec af bash -c "airflow connections add spark_conn --conn-type spark --conn-host spark://spark-master --conn-port 7077"

docker compose exec af bash -c "airflow connections add minio_conn \
--conn-type aws \
--conn-extra '{\"aws_access_key_id\": \"VoEYUMK3DiUGx8SmZsqW\", \"aws_secret_access_key\": \"br3PCqPUANQhNyY0xvUDQXocD8MON9kga7A4fGJu\", \"endpoint_url\": \"http://minio:9000\"}'"

docker compose exec af bash -c "airflow connections add 'pg_conn' \
    --conn-type postgres \
    --conn-host db \
    --conn-schema gh_archive \
    --conn-login postgres \
    --conn-password postgres \
    --conn-port 5432"

chmod 777 /var/run/docker.sock