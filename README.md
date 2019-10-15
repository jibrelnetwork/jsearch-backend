# Jsearch backend services

This is the core of Jsearch. 

## Description

Jsearch Backend includes following components: 

- **API** — provides access to blockchain data stored in main database and acts
as Web3 API proxy.
- **Blocks Syncer** — grabs blockchain data from RawDB and puts it into MainDB.
- **Pending TXs Syncer** — grabs TX pool data from RawDB and puts it into
MainDB.

## Dependencies

Jsearch Backend depends on following services:
- Jsearch Raw DB populated by [Geth Fork](https://github.com/jibrelnetwork/go-ethereum)
- [Jsearch Contracts Service](https://github.com/jibrelnetwork/jsearch-contracts)
- [Jsearch Compiler](https://github.com/jibrelnetwork/jsearch-compiler)


## Prerequisites

* [Docker](https://docs.docker.com/install/)
* [Docker Compose](https://docs.docker.com/compose/install/)

## Installation
```bash
docker-compose build
```

## Configuration

Following environmental variables are used by the project and can be configured:
* `LOG_LEVEL=INFO`
* `SENTRY_DSN=""`
* `JSEARCH_SYNC_PARALLEL="10"`
* `JSEARCH_MAIN_DB="postgres://postgres:postgres@main_db/jsearch_main"`
* `JSEARCH_RAW_DB="postgres://postgres:postgres@raw_db/jsearch_raw"`
* `JSEARCH_CONTRACTS_API="http://contracts:8080"`
* `JSEARCH_COMPILER_API="http://compiler"`
* `JSEARCH_API_ENABLE_RESET_LOGS_PROCESSING="1"`
* `ENH_NODE_URL="https://main-node.jwallet.network"`
* `DOCKERIZE_VERSION="v0.6.1"`
* `ETHERSCAN_API_URL`
* `ETHERSCAN_API_KEY`
* `INFURA_API_URL`
* `INFURA_API_KEY`
* `JWALLET_API_URL`

## API

Swagger docs for API is available by `{hostname}/docs/index.html` URL.

## Development

Use `docker-compose` and `make` to create and run development environment.

### Updating requirments

Use [pip-tools](https://github.com/jazzband/pip-tools#example-usage-for-pip-compile).
```.env
make shell_root
>>> pip compile requirements.in
```

### Running components 

Components can be run either from the [meta repository](https://github.com/jibrelnetwork/jsearch)
or from within the shell:

```bash
docker-compose run --rm tests_shell app
docker-compose run --rm tests_shell jsearch-syncer
docker-compose run --rm tests_shell jsearch-syncer-pending
```

### Migrations
* `make new_db_migration` creates a new MainDB migration.
* `make db_migrate` migrates MainDB.

## Devtools

### Shell

To spawn a dev shell, execute:
```bash
make shell
```

### Code validation
To check the code, you can execute the following:
```bash
make validate
```

The `validate` rule builds containers, checks the code style and runs tests. If
you want to execute commands separately, check the "Code validation subrules"
section below.

### Code validation subrules

#### Linters
To run [flake8](http://flake8.pycqa.org/en/latest/), execute the following
command:
```bash
make lint
```

`flake8` configuration can be customized in `.flake8` file.

#### Running tests
[pytest](https://pytest.org) is used as a testing framework. To run test suite,
execute: 
```bash
make test
```

### Git hooks

Optionally, you can add a pre push hook to validate the code before each push:

```bash
ln -s $(pwd)/pre-push.sh ../.git/modules/jsearch-backend/hooks/pre-push  # jsearch-backend as a git submodule.
ln -s $(pwd)/pre-push.sh .git/hooks/pre-push  # jsearch-backend as a standalone repo.
```

To push the code without a hook, `git push --no-verify` can be used (e.g.
you've already run `make validate` before push).

## Author

dev@jibrel.network