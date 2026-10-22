DATE ?= 2026-01-15
CONF ?= {}
.DEFAULT_GOAL := up

.PHONY: help up down ps run smoke failure quality-failure report verify logs

help:
	@printf '%s\n' \
	  'Usage: make <command> [DATE=YYYY-MM-DD]' \
	  '' \
	  'Commands:' \
	  '  help             Show available commands' \
	  '  up               Build and start services; wait until ready' \
	  '  down             Stop services and keep volumes' \
	  '  ps               Show service status' \
	  '  run              Run the pipeline for DATE' \
	  '  smoke            Check all layers, repeat runs and two dates' \
	  '  failure          Run with a deliberate ingestion failure' \
	  '  quality-failure  Run with a deliberate quality failure' \
	  '  report           Print the CSV report for DATE' \
	  '  verify           Check and print row counts for DATE' \
	  '  logs             Follow Airflow container logs' \
	  '' \
	  'Default DATE: $(DATE)' \
	  'Example: make run DATE=2026-01-15' \
	  'Optional run configuration: make run CONF='\''{"fail_ingest":true}'\'''

up:
	docker compose up -d --build --wait

down:
	docker compose down

ps:
	docker compose ps

run:
	docker compose exec -T airflow airflow dags test bank_batch $(DATE) --conf '$(CONF)'

smoke:
	docker compose exec -T airflow python /opt/airflow/tests/smoke.py $(DATE)

failure:
	$(MAKE) run CONF='{"fail_ingest":true}'

quality-failure:
	$(MAKE) run CONF='{"fail_quality":true}'

report:
	docker compose exec -T airflow bash -c 'cat "$$REPORT_DIR/daily_$(DATE).csv"'

verify:
	docker compose exec -T airflow python -c 'from tests.smoke import counts; counts("$(DATE)")'

logs:
	docker compose logs -f airflow
