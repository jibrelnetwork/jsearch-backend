# jSearch backend services

This is a core of jSearch 

# Description

jSearch backend services includes following components: 

- Main DB
- syncer
- post-processing
- jSearch api

Main DB - PostgreSQL database, stores processed and structured blockchain data

Syncer component grabs blockchain data from RAW database and puts it into MAIN database

Postprocessing component performs raw blockchain data processing - transaction and logs decoding,
token transfers detection, token holders balances updates 

API component is public web API server - provides access to blockchain data stored in main database and acts as Web3 API proxy


# Dependencies

jSearch Backend depends on following services:
- jSearch Raw DB (geth fork) [https://github.com/jibrelnetwork/go-ethereum]
- jSearch Contracts Service [https://github.com/jibrelnetwork/jsearch-contracts]
- jSearch Compiler [https://github.com/jibrelnetwork/jsearch-compiler]


# Install


## Prerequisites

Ubuntu 16.04, git installed

```
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt-get update
sudo apt-get install python3.6 python3.6-dev postgresql-client-9.5 libssl-dev python3-pip
```


## Build

```
git clone git@github.com:jibrelnetwork/jsearch-contracts.git
cd jsearch-contracts
pip install -r requirements.txt
pip install -e .
```

## Configuration

List of environ vars:
```
JSEARCH_MAIN_DB (default postgres://localhost/jsearch_main)
JSEARCH_RAW_DB (default postgres://localhost/jsearch_raw)
ETH_NODE_URL (default https://main-node.jwallet.network)
JSEARCH_COMPILER_API (default http://localhost:8101)
JSEARCH_CONTRACTS_API (default http://localhost:8101)
JSEARCH_SYNC_PARALLEL (default 10) - number of blocks to sync in parallel
```

# Run components

## Before run
Apply database migrations:
```
python manage.py upgrade head -db=postgresql://dbuser@localhost:5433/jsearch_main
```

## Run Syncer:
```
jsearch-syncer
```

## Run Post processing:
```
jsearch-post-processing logs  # run logs processing/decoding
jsearch-post-processing transfers  # run operations (Token Transfers) processing - will update token balances
```

## Run API server:
```
gunicorn  --bind 0.0.0.0:8081 jsearch.api.app:make_app --worker-class aiohttp.worker.GunicornWebWorker
```


# API

Swagger docs for API is available by {hostname}/docs/index.html URL


# Development

Use docker-compose to create and run development environment:
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

