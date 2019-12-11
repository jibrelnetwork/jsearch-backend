PROJECT_NAME=jsearch

shell:
	docker-compose run --rm dev_shell bash

shell_root:
	docker-compose run -u root dev_shell bash

build:
	docker-compose build dev

lint:
	docker-compose run --rm dev_shell flake8 .

lock:
	@docker-compose run -u root --rm dev_shell pip-compile

mypy:
	docker-compose run --rm dev_shell mypy .

test:
	docker-compose run --rm dev pytest

validate:
	make build
	make test

new_db_migration:
	docker-compose run --entrypoint python dev_shell manage.py revision -db=postgres://postgres:postgres@test_main_db/jsearch-main -m "$(msg)"

db_migrate:
	docker-compose run --entrypoint python dev_shell manage.py upgrade head -db=postgres://postgres:postgres@test_main_db/jsearch-main
