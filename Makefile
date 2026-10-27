DC = docker compose
PSQL = $(DC) exec -T postgres sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" "$$@"' --

.PHONY: up down reset logs sync-full sync-incremental sync-reset topics events-ownership events-counterparties dlq db-check psql

up:                     ## поднять инфраструктуру и сервисы
	$(DC) up -d --build

down:
	$(DC) down

reset:                  ## удалить все данные (Kafka, PostgreSQL, метки синхронизации)
	$(DC) down -v

logs:
	$(DC) logs -f integration-service consumer-service

sync-full:              ## полная синхронизация 1С -> Kafka
	$(DC) exec integration-service ./sync --mode full

sync-incremental:       ## только изменённые с прошлого запуска записи
	$(DC) exec integration-service ./sync --mode incremental

sync-reset:             ## сбросить метки и выгрузить всё заново
	$(DC) exec integration-service ./sync --mode full --reset-state

topics:
	$(DC) exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe

events-ownership:
	$(DC) exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
		--topic 1c.ownership_forms.v1 --from-beginning --property print.key=true --timeout-ms 5000

events-counterparties:
	$(DC) exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
		--topic 1c.counterparties.v1 --from-beginning --property print.key=true --timeout-ms 5000

dlq:
	$(DC) exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
		--whitelist '1c\..*\.dlq' --from-beginning --property print.headers=true --timeout-ms 5000

db-check:               ## выборки для проверки данных в PostgreSQL
	$(PSQL) -f /sql/checks.sql

psql:
	$(DC) exec postgres sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'
