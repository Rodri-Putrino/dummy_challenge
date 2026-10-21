#!/usr/bin/env bash
set -euo pipefail
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  --set=airflow_password="$AIRFLOW_DB_PASSWORD" <<'SQL'
CREATE USER airflow WITH PASSWORD :'airflow_password';
CREATE DATABASE airflow OWNER airflow;
SQL
