PROJECT_NAME=jsearch

shell:
	docker-compose run --rm --entrypoint bash syncer

shell_dev:
	docker-compose run --rm --entrypoint bash dev

build:
	docker-compose build api

lock:
	docker-compose run --rm --entrypoint "poetry lock" dev

build_dev:
	docker-compose build api
	docker-compose build dev

lint:
	docker-compose run --rm --entrypoint flake8 dev

test:
	docker-compose run --rm --entrypoint pytest dev

validate:
	make build_dev
	make lint
	make test

main_db_shell:
	docker-compose exec -u postgres main_db psql jsearch_main

raw_db_shell:
	docker-compose exec -u postgres raw_db psql jsearch_raw

kafka_shell:
	docker-compose exec kafka bash

kafka_groups:
	docker-compose exec kafka kafka-consumer-groups.sh --bootstrap-server kafka:9092 --list

kafka_group_stat:
	docker-compose exec kafka kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group ${group}

kafka_topics:
	docker-compose exec kafka kafka-topics.sh --list --zookeeper zookeeper:2181

kafka_read_topic:
	docker-compose exec kafka kafka-console-consumer.sh --topic ${topic} --from-beginning --bootstrap-server kafka:9092

kafka_reset_offset:
	docker-compose exec kafka kafka-consumer-groups.sh --bootstrap-server kafka:9092 --group ${group} --topic ${topic} --reset-offsets --to-earliest --execute

new_db_migration:
	docker-compose run --entrypoint python dev manage.py revision -db=postgres://postgres:postgres@main_db/jsearch_main -m "$(msg)"

db_migrate:
	docker-compose run --entrypoint python dev manage.py upgrade head -db=postgres://postgres:postgres@main_db/jsearch_main
