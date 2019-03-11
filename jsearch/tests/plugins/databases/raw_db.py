import logging
import os
from functools import partial
from pathlib import Path
import json

import pytest
from sqlalchemy import create_engine, MetaData

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

SQL_FOLDER = Path(__file__).parent / "sql"
DUMPS_FOLDER = Path(__file__).parent / "dumps"


def setup_database(connection_string):
    engine = create_engine(connection_string)
    with engine.connect() as db:
        sql = SQL_FOLDER / "raw_db_initial.sql"
        db.execute(sql.read_text())


def teardown_database(connection_string):
    engine = create_engine(connection_string)
    with engine.connect() as db:
        truncate(db)


def truncate(db):
    tables = [
        "headers",
        "bodies",
        "pending_transactions",
        "receipts",
        "accounts",
        "rewards",
        "reorgs",
        "chain_splits",
        "internal_transactions"
    ]
    for table in tables:
        db.execute(f"TRUNCATE {table};")


@pytest.fixture(scope="session")
def raw_db_connection_string():
    return os.environ.get(
        'JSEARCH_RAW_DB_TEST',
        "postgres://postgres:postgres@test_raw_db/jsearch_raw?sslmode=disable"
    )


@pytest.fixture(scope="session", autouse=True)
def raw_db_create_tables(request, raw_db_connection_string):
    setup_database(connection_string=raw_db_connection_string)

    finalizer = partial(teardown_database, raw_db_connection_string)
    request.addfinalizer(finalizer)


@pytest.fixture(scope="function")
def raw_db_name(raw_db_connection_string):
    return raw_db_connection_string.split('/')[-1]


@pytest.fixture()
def raw_db(raw_db_connection_string):
    engine = create_engine(raw_db_connection_string)
    conn = engine.connect()
    yield conn
    conn.close()


@pytest.fixture
def clean_test_raw_db(db):
    return partial(truncate, db)


@pytest.fixture
def raw_db_sample(raw_db_connection_string):
    engine = create_engine(raw_db_connection_string)
    meta = MetaData()
    meta.reflect(bind=engine)
    sample_data = {}
    for t in meta.sorted_tables:
        p = DUMPS_FOLDER / 'raw_db_sample' / f'{t}.json'
        if not p.exists():
            continue
        table_data = json.load(p.open())
        engine.execute(t.insert(), *table_data)
        sample_data[t.name] = table_data
    yield sample_data
    truncate(engine)
