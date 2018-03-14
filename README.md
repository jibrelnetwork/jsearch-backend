# jsearch-syncer

## Installation
pip install -r requirements.txt
pip install -e .

## DB migration
yoyo apply --database postgresql://dbuser@localhost:5433/jsearch_main ./jsearch_syncer/migrations

## Starting service
jsearch-syncer --main-db=postgresql://dbuser:passwd@localhost:5433/jsearch_main --raw-db=postgres://dbuser:passwd@localhost:5553/jsearch
