# jSearch backend services

# Description

jSearch backend services includes following components: 

- syncer
- api
- celery task queue
- esparser - scrapy-based parser for etherscan

Syncer grabs blockchain data from RAW database and puts it into MAIN database

API component is public web API server - implements access to blockchain data stored in main database and acts as Web3 API proxy

Celery - for warious background tasks

Esparser - used to get verified contracts data from etherscan.io

## Prerequisites

Ubuntu 16.04, git installed

```
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt-get update
sudo apt-get install python3.6 python3.6-dev postgresql-client-9.5 libssl-dev python3-pip
```

Contract Compilation service requires shared directory for `solc` installation, use env var `SOLC_BASE_INSTALL_PATH` to define this directory path (default `~/.py-solc`)

## Installation

```
pip install -r requirements.txt
pip install -e .
```

## Configuration

List of environ vars:
```
JSEARCH_MAIN_DB (default postgres://localhost/jsearch_main)
JSEARCH_RAW_DB (default postgres://localhost/jsearch_raw)
ETH_NODE_URL (default https://main-node.jwallet.network)
JSEARCH_CELERY_BROKER (default redis://localhost:6379/0)
JSEARCH_CELERY_BACKEND (default redis://localhost:6379/0)
JSEARCH_COMPILER_API (default http://localhost:8101)
JSEARCH_CONTRACTS_API (default http://localhost:8101)
JSEARCH_SYNC_PARALLEL (default 10) - number of blocks to sync in parallel
```

## DB migration

make new revision
```
python manage.py revision -db=postgresql://dbuser@localhost:5433/jsearch_main -m "Initial"
```

run migration on database
```
python manage.py upgrade head -db=postgresql://dbuser@localhost:5433/jsearch_main
```

## Running tests
    
First you need blank PostgreSQL database for tests

Then run:

```JSEARCH_MAIN_DB_TEST=postgresql://localhost/jsearch_main_test JSEARCH_MAIN_DB=postgres://localhost/jsearch_main_test pytest -v```


## Starting services

### Syncer:
```
jsearch-syncer
```

### Post processing:
```
jsearch-post-processing --wait
jsearch-post-processing operations --wait
```

### API:
```
gunicorn  --bind 0.0.0.0:8081 jsearch.api.app:make_app --worker-class aiohttp.worker.GunicornWebWorker
```

### Celery
```
celery worker -A jsearch.common.celery
```

## Docker compose

### Running components 

```
docker-compose up -d api
docker-copmose up -d syncer
```

### Running migrations
```
docker-compose run --entrypoint python syncer manage.py revision -db=postgres://postgres:postgres@main_db/jsearch_main -m "Initial"
docker-compose run --entrypoint python syncer manage.py upgrade head -db=postgres://postgres:postgres@main_db/jsearch_main
```

### Test data
To fill project from raw_db dump need to do:
```
# Download dump of rawdb from https://drive.google.com/drive/folders/1W6Hn4Xwfg-S1kSdrp5MGibOVjjT9rT6T?usp=sharing
# There are two files: 
# - 001-initial.sql
# - rawdata.tar
# to folder ./backups

cd ./backups && gunzip -c rawdata.tar | tar xopf - && cd ..;
docker-compose up -d raw_db;
docker-compose exec -T -u postgres raw_db psql jsearch_raw;
cat ./backups/001-initial.sql | docker-compose exec -T -u postgres raw_db psql jsearch_raw;
echo "COPY rewards FROM '/backups/rewards.tsv';" | docker-compose exec -T -u postgres raw_db psql jsearch_raw;
echo "COPY bodies FROM '/backups/bodies.tsv';" | docker-compose exec -T -u postgres raw_db psql jsearch_raw;
echo "COPY headers FROM '/backups/headers.tsv';" | docker-compose exec -T -u postgres raw_db psql jsearch_raw;
echo "COPY accounts FROM '/backups/accounts.tsv';" | docker-compose exec -T -u postgres raw_db psql jsearch_raw;
echo "COPY internal_transactions FROM '/backups/internal_transactions.tsv';" | docker-compose exec -T -u postgres raw_db psql jsearch_raw;
echo "COPY receipts FROM '/backups/receipts.tsv';" | docker-compose exec -T -u postgres raw_db psql jsearch_raw;
```
After need to sync main database.
Before do it - need to up contracts services.
```
cd ..
git clone https://github.com/jibrelnetwork/jsearch-contracts
cd jsearch-contracts
docker-compose up -d contracts
```
After it - run syncer.
```
docker-compose run -d syncer --sync-range=5000000-
```

### Running tests

#### From docker-compose:
```
docker-compose rm --rm tests
```

#### From shell:
```
pytest # all test
pytest -m "live_chain" #only end to end tests
pytest -v -m "live_chain" #only unit tests with
```

#### Notes:
 - ensure to build docker image for geth-fork 
```
cd ./docker/geth-fork
./build.sh
```

## Author

dev@jibrel.network

