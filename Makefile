PROJECT_NAME=jsearch

shell:
	docker-compose run --rm --entrypoint bash syncer

shell_tests:
	docker-compose run --rm --entrypoint bash tests

build:
	docker-compose build api

build_tests:
	docker-compose build api
	docker-compose build tests

main_db_shell:
	docker-compose exec -u postgres main_db psql jsearch_main

raw_db_shell:
	docker-compose exec -u postgres raw_db psql jsearch_raw

