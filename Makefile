PROJECT_NAME=jsearch

shell:
	docker-compose run --rm tests_shell bash

build:
	docker-compose build tests

lint:
	docker-compose run --rm tests flake8 .

test:
	docker-compose run --rm tests pytest

validate:
	make build
	make test

new_db_migration:
	docker-compose run --entrypoint python tests_shell manage.py revision -db=postgres://postgres:postgres@test_main_db/jsearch-main -m "$(msg)"

db_migrate:
	docker-compose run --entrypoint python tests_shell manage.py upgrade head -db=postgres://postgres:postgres@test_main_db/jsearch-main
