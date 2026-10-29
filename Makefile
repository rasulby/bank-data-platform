DURATION ?= 30
RATE ?= 1
DATE ?= $(shell date -u +%F)
CONF ?= {}
.DEFAULT_GOAL := help
COMPOSE = docker compose
CLIENT = $(COMPOSE) exec -T airflow python /opt/airflow/scripts/job_client.py

.PHONY: help up setup down clean ps run smoke verify report cdc-demo cdc-source live kafbat realtime-start realtime-stop realtime-status realtime-logs timings failure quality-failure monitoring bi logs worker-logs lint
help:
	@printf '%s\n' \
	  'Usage: make <command> [DATE=YYYY-MM-DD]' \
	  '' \
	  '  up               Build and start core services, register CDC' \
	  '  setup            Create bucket/source and register CDC (safe to repeat)' \
	  '  run              Run the Airflow pipeline for DATE' \
	  '  verify           Compare source, lake, warehouse and report' \
	  '  report           Print the daily CSV report' \
	  '  smoke            Run two dates, repeat one, check every layer' \
	  '  kafbat           Start Kafka and CDC browser at localhost:8085' \
	  '  realtime-start   Start continuous Spark processing (after a batch bootstrap)' \
	  '  realtime-stop    Finish current work and stop the continuous writer' \
	  '  realtime-status  Show process state and last completed microbatch' \
	  '  realtime-logs    Show the continuous Spark process log' \
	  '  timings          Show UTC timestamps and measured latency for DATE' \
	  '  live             Generate live source inserts/updates/deletes (30 sec)' \
	  '  cdc-source       Change source only; inspect events before running Spark' \
	  '  cdc-demo         Update and insert source rows, rerun and verify' \
	  '  failure          Demonstrate ingestion failure and blocked downstream tasks' \
	  '  quality-failure  Demonstrate a failing quality check' \
	  '  monitoring       Start Prometheus and Grafana (optional)' \
	  '  bi               Start Superset with a provisioned dashboard (optional)' \
	  '  ps               Show all project containers' \
	  '  logs             Follow Airflow logs' \
	  '  worker-logs      Follow external job service logs' \
	  '  down             Stop all profiles; preserve data' \
	  '  clean            DELETE this project containers and volumes (CONFIRM=yes)' \
	  '  lint             Run local lint, format and compile checks' \
	  '' \
	  'Example: make run DATE=2026-01-15'

up:
	@test -f .env || cp .env.example .env
	$(COMPOSE) up -d --build --wait
	$(MAKE) setup

setup:
	$(CLIENT) setup --date $(DATE)

down:
	$(COMPOSE) --profile monitoring --profile bi down --remove-orphans

clean:
	@test "$(CONFIRM)" = yes || { echo 'This deletes all v2 demo data. Run: make clean CONFIRM=yes'; exit 1; }
	$(COMPOSE) --profile monitoring --profile bi down --volumes --remove-orphans

ps:
	$(COMPOSE) --profile monitoring --profile bi ps

run:
	$(COMPOSE) exec -T airflow python /opt/airflow/tests/airflow_smoke.py $(DATE) '$(CONF)'

verify:
	$(CLIENT) verify --date $(DATE)

report:
	$(COMPOSE) exec -T worker sh -c 'cat "$$REPORT_DIR/daily_$(DATE).csv"'

smoke:
	bash scripts/smoke.sh $(DATE)

kafbat:
	$(COMPOSE) up -d --wait kafbat

realtime-start:
	$(COMPOSE) exec -T airflow python /opt/airflow/scripts/realtime_client.py start

realtime-stop:
	$(COMPOSE) exec -T airflow python /opt/airflow/scripts/realtime_client.py stop

realtime-status:
	$(COMPOSE) exec -T airflow python /opt/airflow/scripts/realtime_client.py status

realtime-logs:
	$(COMPOSE) exec -T worker python -c 'import json; from pathlib import Path; state=json.loads(Path("/state/realtime.json").read_text()); print(Path("/state/jobs",state["job_id"]+".log").read_text()[-24000:])'

timings:
	$(COMPOSE) exec -T worker python -m jobs.timings --date $(DATE)

live:
	$(COMPOSE) exec -T worker python -m jobs.live_source --date $(DATE) --duration $(DURATION) --rate $(RATE)

cdc-source:
	$(CLIENT) change --date $(DATE)

cdc-demo:
	$(CLIENT) change --date $(DATE)
	$(MAKE) run DATE=$(DATE)
	$(MAKE) verify DATE=$(DATE)

failure:
	$(MAKE) run DATE=$(DATE) CONF='{"fail_ingest":true}'

quality-failure:
	$(MAKE) run DATE=$(DATE) CONF='{"fail_quality":true}'

monitoring:
	$(COMPOSE) --profile monitoring up -d --wait prometheus grafana

bi:
	$(COMPOSE) --profile bi up -d --build --wait superset

logs:
	$(COMPOSE) logs -f airflow

worker-logs:
	$(COMPOSE) logs -f worker

lint:
	ruff check jobs worker scripts orchestration tests infra/superset
	ruff format --check jobs worker scripts orchestration tests infra/superset
	python3 -m compileall -q jobs worker scripts orchestration tests infra/superset
	$(COMPOSE) --env-file .env.example config --quiet
