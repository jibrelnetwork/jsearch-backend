# jSearch backend services

# Description

jSearch backend services incude 3 main components: 

- syncer
- api
- node proxy

Syncer grabs blockchain data from RAW database and puts it into MAIN database

API component is public web API server - implements access to blockchain data stored in main database and acts as Web3 API proxy

Node proxy service - proxy and load balancer for web3 API calls

## Installation
pip install -e .

## DB migration
yoyo apply --database postgresql://dbuser:dbpass@localhost:5432/jsearch_main ./jsearch/common/migrations


## Starting services

###Syncer:
jsearch-syncer --main-db=postgresql://dbuser:passwd@localhost:5432/jsearch_main --raw-db=postgres://dbuser:passwd@localhost:5432/jsearch_raw

###API:
gunicorn  --bind 0.0.0.0:8081 jsearch.api.app:app --worker-class aiohttp.worker.GunicornWebWorker