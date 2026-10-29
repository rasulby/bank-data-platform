#!/usr/bin/env bash
set -euo pipefail
airflow db migrate
airflow users create --username "$AIRFLOW_ADMIN_USER" --password "$AIRFLOW_ADMIN_PASSWORD" \
  --firstname Local --lastname User --role Admin --email local@example.com || true
# No deferrable tasks are used, so a triggerer is not needed.
# Logs are on the same volume as the webserver; no extra log server is needed.
airflow scheduler --skip-serve-logs &
scheduler_pid=$!
airflow webserver &
webserver_pid=$!
trap 'kill "$scheduler_pid" "$webserver_pid" 2>/dev/null || true' EXIT TERM INT
# Exit if either essential process dies, so container health is not misleading.
wait -n "$scheduler_pid" "$webserver_pid"
exit 1
