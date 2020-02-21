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

migration:
	docker-compose run dev_shell python manage.py create -m "$(msg)"

migrate:
	docker-compose run dev_shell python manage.py up
