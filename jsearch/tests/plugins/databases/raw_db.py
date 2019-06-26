import json
import logging
import os

import pytest
from functools import partial
from pathlib import Path
from sqlalchemy import create_engine, MetaData

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

SQL_FOLDER = Path(__file__).parent / "sql"
DUMPS_FOLDER = Path(__file__).parent / "dumps"

tables = [
    "headers",
    "bodies",
    "pending_transactions",
    "receipts",
    "accounts",
    "rewards",
    "reorgs",
    "chain_splits",
    "chain_events",
    "internal_transactions"
]


def setup_database(connection_string):
    engine = create_engine(connection_string)
    with engine.connect() as db:
        sql = SQL_FOLDER / "raw_db_initial.sql"
        db.execute(sql.read_text())


def teardown_database(connection_string):
    engine = create_engine(connection_string)
    with engine.connect() as db:
        for table in tables:
            db.execute(f"DROP TABLE IF EXISTS {table}")


def truncate(db):
    for table in tables:
        db.execute(f"TRUNCATE {table};")


@pytest.fixture(scope="session")
def raw_db_dsn():
    return os.environ.get(
        'JSEARCH_RAW_DB_TEST',
        "postgres://postgres:postgres@test_raw_db/jsearch_raw?sslmode=disable"
    )


@pytest.fixture(scope='function', autouse=True)
def mock_settings(mocker, raw_db_dsn):
    mocker.patch('jsearch.settings.JSEARCH_RAW_DB', raw_db_dsn)


@pytest.fixture(scope="session", autouse=True)
def raw_db_create_tables(request, raw_db_dsn):
    setup_database(connection_string=raw_db_dsn)

    finalizer = partial(teardown_database, raw_db_dsn)
    request.addfinalizer(finalizer)


@pytest.fixture(scope="function")
def raw_db_name(raw_db_dsn):
    return raw_db_dsn.split('/')[-1]


@pytest.fixture()
def raw_db(raw_db_dsn):
    engine = create_engine(raw_db_dsn)
    conn = engine.connect()
    yield conn
    conn.close()


@pytest.fixture
def clean_test_raw_db(db):
    return partial(truncate, db)


def load_sample(engine, path):
    meta = MetaData()
    meta.reflect(bind=engine)
    sample_data = {}
    for t in meta.sorted_tables:
        p = path / f'{t}.json'
        if not p.exists():
            continue
        table_data = json.load(p.open())
        engine.execute(t.insert(), *table_data)
        sample_data[t.name] = table_data
    return sample_data


@pytest.fixture
def raw_db_sample(raw_db_dsn):
    engine = create_engine(raw_db_dsn)
    yield load_sample(engine, DUMPS_FOLDER / 'raw_db_sample')
    truncate(engine)


@pytest.fixture
def raw_db_split_sample(raw_db_dsn):
    engine = create_engine(raw_db_dsn)
    yield load_sample(engine, DUMPS_FOLDER / 'raw_db_split_sample')
    truncate(engine)


@pytest.fixture(scope='function', autouse=True)
def truncate_raw_db(raw_db):
    truncate(raw_db)
