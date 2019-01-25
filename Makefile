PROJECT_NAME=jsearch

shell:
	docker-compose run --rm --entrypoint bash syncer

shell_dev:
	docker-compose run --rm --entrypoint bash dev

build:
	docker-compose build api

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

