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
SOLC_BASE_INSTALL_PATH (default ~/.py-solc)
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


Test environ should have `solc` revision 9cf6e910 installed:

```python -m solc.install 9cf6e910```

Then run:

```JSEARCH_MAIN_DB_TEST=postgresql://localhost/jsearch_main_test JSEARCH_MAIN_DB=postgres://localhost/jsearch_main_test pytest -v```


## Starting services

### Syncer:

```
jsearch-syncer
```

### API:

```
gunicorn  --bind 0.0.0.0:8081 jsearch.api.app:make_app --worker-class aiohttp.worker.GunicornWebWorker
```

### Scraper

```
jsearch-syncer
```

### Celery

```
celery worker -A jsearch.common.celery
```

There in 3 celery queues: `scrapy` - for periodic scraping of Etherscan, `solc` - for solc installation tasks, `default` - other tasks

## Author

dev@jibrel.network

