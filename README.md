# jSearch backend services

This is a core of jSearch 

## Description

jSearch Backend services includes following components: 

- **Main DB** — PostgreSQL database, stores processed and structured blockchain data
- **syncer** — grabs blockchain data from RAW database and puts it into MAIN database
- **post-processing** — performs raw blockchain data processing - transaction and
logs decoding, token transfers detection, token holders balances updates
- **API** — provides access to blockchain data stored in main database and acts as
Web3 API proxy

## Dependencies

jSearch Backend depends on following services:
- jSearch Raw DB (geth fork) [https://github.com/jibrelnetwork/go-ethereum]
- jSearch Contracts Service [https://github.com/jibrelnetwork/jsearch-contracts]
- jSearch Compiler [https://github.com/jibrelnetwork/jsearch-compiler]


## Prerequisites

* [Docker](https://docs.docker.com/install/)
* [Docker Compose](https://docs.docker.com/compose/install/)

## Installation
```
docker-compose build
```

## Configuration

Following environmental variables are used by the project and can be configured:
* `LOG_LEVEL=INFO`
* `RAVEN_DSN=""`
* `JSEARCH_SYNC_PARALLEL="10"`
* `JSEARCH_MAIN_DB="postgres://postgres:postgres@main_db/jsearch_main"`
* `JSEARCH_RAW_DB="postgres://postgres:postgres@raw_db/jsearch_raw"`
* `JSEARCH_CELERY_BROKER="redis://redis/0"`
* `JSEARCH_CELERY_BACKEND="redis://redis/1"`
* `JSEARCH_CONTRACTS_API="http://contracts:8080"`
* `JSEARCH_COMPILER_API="http://compiler"`
* `JSEARCH_API_ENABLE_RESET_LOGS_PROCESSING="1"`
* `ENH_NODE_URL="https://main-node.jwallet.network"`
* `KAFKA_BOOTSTRAP_SERVERS=""`
* `POETRY_VERSION="0.12.12"`

## API

Swagger docs for API is available by `{hostname}/docs/index.html` URL.


## Development

Use `docker-compose` and `make` to create and run development environment:

### Running components 

```
docker-compose up -d api
docker-copmose up -d syncer
docker-copmose up -d post_processing
```

### Running migrations
```
make new_db_migration
make db_migrate
```

### Test data
To fill project from `raw_db` dump, you'll need to do download dump of rawdb
from https://drive.google.com/drive/folders/1W6Hn4Xwfg-S1kSdrp5MGibOVjjT9rT6T?usp=sharing
to folder `./backups`. There are two files: 

- 001-initial.sql
- rawdata.tar

Populate local `raw_db`:
```
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

Then, run contracts services...
```
cd ..
git clone https://github.com/jibrelnetwork/jsearch-contracts
cd jsearch-contracts
docker-compose up -d contracts
```

...and syncer:
```
docker-compose run -d syncer --sync-range=5000000-
```

### Kafka inspection

To inspect Kafka, there're several `make` rules, namely:
* `kafka_shell`
* `kafka_groups`
* `kafka_group_stat`
* `kafka_topics`
* `kafka_read_topic`
* `kafka_reset_offset`

### PostgreSQL inspection

You can spawn `psql` shell with following shortcuts:
* `make raw_db_shell`
* `make main_db_shell`

## Devtools

### Shell

To spawn a dev shell, execute:
```
make shell_dev
```

### Dependencies

*poetry* is a dependency manager of choice. To add a new dependency or update an
old one, you can either spawn a dev shell with a *poetry* installed and work
from here (see the [docs](http://poetry.eustace.io/)) or you can manipulate
`pyproject.toml` directly. After you've updated requirements of a project, run
`make lock` to lock dependencies.

### Code validation
To check the code, you can execute the following:
```
make validate
```

The `validate` rule checks the code style, typechecks it and runs tests. If you
want to execute commands separately, check the "Code validation subrules"
section below.

### Code validation subrules

#### Linters
To run [flake8](http://flake8.pycqa.org/en/latest/), execute the following
command:
```
make lint
```

The all settings connected with a `flake8` you can customize in `.flake8`.

#### Running tests
[pytest](https://pytest.org) is used as a testing framework. To run test suite,
execute: 
```
make test
```

### Git hooks

Add a pre push hook to validate the code before each push:

```bash
$ ln -s $(pwd)/pre-push.sh ../.git/modules/jsearch-backend/hooks/pre-push  # jsearch-backend as a git submodule.
$ ln -s $(pwd)/pre-push.sh .git/hooks/pre-push  # jsearch-backend as a standalone repo.
```

To push the code without a hook, `git push --no-verify` can be used (e.g.
you've already run `make validate` before push).

## Author

dev@jibrel.network
