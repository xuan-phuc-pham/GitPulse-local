#!/bin/bash

HOST="localhost"
PORT="5432"
USER="postgres"
DB="airflow_db"
PASSWORD="postgres"

psql postgresql://postgres:postgres@localhost:5432/airflow_db -c "TRUNCATE TABLE connection;"

docker compose down