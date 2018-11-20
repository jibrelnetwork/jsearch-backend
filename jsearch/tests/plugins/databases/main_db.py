import logging
import os
from functools import partial

import pytest
from sqlalchemy import create_engine

log = logging.getLogger(__name__)


def setup_database(connection_string):
    from jsearch.common.alembic_utils import upgrade
    upgrade(connection_string, 'head')


def teardown_database(connection_string):
    from jsearch.common.alembic_utils import downgrade
    downgrade(connection_string, 'base')


@pytest.fixture(scope="session")
def db_connection_string():
    return os.environ.get('JSEARCH_MAIN_DB_TEST', "postgres://postgres:postgres@test_db/jsearch_main_test")


@pytest.fixture(scope="function")
def db_name(db_connection_string):
    return db_connection_string.split('/')[-1]


@pytest.fixture(scope='session', autouse=True)
def setup_database_fixture(request, db_connection_string, pytestconfig):
    setup_database(db_connection_string)

    finalizer = partial(teardown_database, db_connection_string)
    request.addfinalizer(finalizer)


@pytest.fixture(scope='function')
def db(db_connection_string):
    engine = create_engine(db_connection_string)
    conn = engine.connect()
    yield conn
    conn.close()


@pytest.fixture(scope='function')
def fill_db(db, do_truncate_db):
    def _wrapper(dump):
        do_truncate_db()

        from jsearch.common.tables import TABLES
        for table in TABLES:
            records = dump.get(table.name)
            if records:
                db.execute(table.insert(), records)
                logging.info('Fill %s table', table.name)
            else:
                logging.info('Empty data for %s table', table.name)

    return _wrapper


@pytest.fixture()
def do_truncate_db(db):
    def wrapper():
        from jsearch.common.tables import TABLES

        tables = ",".join([table.name for table in TABLES])
        db.execute(f"TRUNCATE {tables};")

    return wrapper


@pytest.fixture(scope='function', autouse=True)
def truncate_db(do_truncate_db):
    do_truncate_db()
