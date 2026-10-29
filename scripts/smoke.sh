#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
day="${1:-2026-01-15}"
smoke_dir=$(mktemp -d "${TMPDIR:-/tmp}/bank-smoke.XXXXXX")
trap 'rm -rf "$smoke_dir"' EXIT
# Date calculation runs inside the worker; no local Python dependency.
next_day=$(docker compose exec -T worker python -c 'from datetime import date,timedelta; import sys; print(date.fromisoformat(sys.argv[1])+timedelta(days=1))' "$day")
make run DATE="$day"
make verify DATE="$day"
docker compose exec -T worker sh -c 'cat "$REPORT_DIR/verify_'"$day"'.json"' > "$smoke_dir/before.json"
make run DATE="$day"
make verify DATE="$day"
docker compose exec -T worker sh -c 'cat "$REPORT_DIR/verify_'"$day"'.json"' > "$smoke_dir/after.json"
# Raw event count grows due to run barriers; business counts must stay the same.
docker compose exec -T worker python -c 'import json,sys; a,b=json.load(sys.stdin); a.pop("raw_events_all_dates"); b.pop("raw_events_all_dates"); assert a==b,(a,b)' < <(printf '[%s,%s]' "$(cat "$smoke_dir/before.json")" "$(cat "$smoke_dir/after.json")")
make run DATE="$next_day"
make verify DATE="$next_day"
make verify DATE="$day"
echo 'SMOKE PASSED: all layers, repeat, two dates, real Airflow runs'
