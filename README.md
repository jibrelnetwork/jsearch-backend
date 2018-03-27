# swagger_client# jSearch backend services

# Description

jSearch backend services incude 3 main components: 

- syncer
- api
- node proxy

Syncer grabs blockchain data from RAW database and puts it into MAIN database

API component is public web API server - implements access to blockchain data stored in main database and acts as Web3 API proxy

Node proxy service - proxy and load balancer for web3 API calls

## Installation
```pip install -e .```

## DB migration
```python manage.py revision -db=postgresql://dbuser@localhost:5433/jsearch_main -m "Initial"```

```python manage.py upgrade head -db=postgresql://dbuser@localhost:5433/jsearch_main```

## Running tests
    
    First you need blank PostgreSQL database for tests

    Then run:

    ```DATABASE_URL=postgresql://dbuser:@localhost:5433/jsearch_main_test JSEARCH_MAIN_DB_TEST=postgresql://dbuser@localhost:5433/jsearch_main_test pytest -v```


## Starting services

### Syncer:
```jsearch-syncer --main-db=postgresql://dbuser:passwd@localhost:5432/jsearch_main --raw-db=postgres://dbuser:passwd@localhost:5432/jsearch_raw```

### API:
```gunicorn  --bind 0.0.0.0:8081 jsearch.api.app:app --worker-class aiohttp.worker.GunicornWebWorker```

 All endpoints do not require authorization.

## Author

dev@jibrel.network

