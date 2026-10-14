
-include .env
export

.DEFAULT_GOAL := help

.PHONY: help up down clean ps psql seed stream connector connector-status topics peek

help: ## Show available commands
	@echo ""
	@echo "Available commands:"
	@echo "-------------------"
	@awk 'BEGIN {FS = ":.*## "}; /^[a-zA-Z_-]+:.*## / {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo ""

up: ## Start all Docker containers
	docker compose up -d

down: ## Stop and remove containers
	docker compose down

clean: ## Stop containers and remove volumes (deletes stored data)
	docker compose down -v

ps: ## Show container status
	docker compose ps

psql: ## Connect to PostgreSQL CLI
	docker compose exec source-db psql -U $(SOURCE_DB_USER) -d $(SOURCE_DB_NAME)

seed: ## Generate and insert sample data
	.venv/bin/python ingestion/generator/seed.py

stream:
	.venv/bin/python ingestion/generator/stream.py --rate 5

connector:
	bash infra/debezium/register.sh

connector-status:
	curl -s http://localhost:8083/connectors/bank-source-connector/status | python3 -m json.tool

topics:
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list

peek:
	docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic bank.public.transactions --from-beginning --max-messages 3 --property print.key=true