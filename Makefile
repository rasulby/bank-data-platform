-include .env
export

.DEFAULT_GOAL := up
.PHONY: up down clean ps psql seed stream connector connector-status topics peek

up:
	docker compose up -d

down:
	docker compose down

clean:
	docker compose down -v

ps:
	docker compose ps

psql:
	docker compose exec source-db psql -U $(SOURCE_DB_USER) -d $(SOURCE_DB_NAME)

seed:
	.venv/bin/python ingestion/generator/seed.py

stream:
	.venv/bin/python ingestion/generator/stream.py --rate 5

connector:
	bash infra/debezium/register.sh

connector-status:
	curl -s http://localhost:$(CONNECT_HOST_PORT)/connectors/$(DEBEZIUM_CONNECTOR_NAME)/status | python3 -m json.tool

topics:
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list

peek:
	docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic $(DEBEZIUM_TOPIC_PREFIX).public.transactions --from-beginning --max-messages 3 --property print.key=true
