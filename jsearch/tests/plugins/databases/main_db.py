import logging
import os
from asyncio import AbstractEventLoop

import aiopg
import dsnparse
import pytest
from aiopg.sa import Engine
from functools import partial
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)


def setup_database(connection_string):
    from jsearch.common.alembic_utils import upgrade
    upgrade(connection_string, 'head')


def teardown_database(connection_string):
    from jsearch.common.alembic_utils import downgrade

    parsed_dsn = dsnparse.parse(connection_string)

    engine = create_engine(connection_string)
    with engine.connect() as db:
        db.execute(
            f"""
            SELECT pg_terminate_backend(pg_stat_activity.pid)
            FROM pg_stat_activity
            WHERE pg_stat_activity.datname = '{parsed_dsn.dbname}'
            AND pid <> pg_backend_pid();
            """
        )
    downgrade(connection_string, 'base')


@pytest.fixture(scope="session")
def db_dsn():
    return os.environ.get('JSEARCH_MAIN_DB_TEST', "postgres://postgres:postgres@test_db/jsearch_main_test")


@pytest.fixture(scope="function")
def db_name(db_dsn):
    return db_dsn.split('/')[-1]


@pytest.fixture(scope='session', autouse=True)
def setup_database_migrations(request, db_dsn, pytestconfig):
    setup_database(db_dsn)

    finalizer = partial(teardown_database, db_dsn)
    request.addfinalizer(finalizer)


@pytest.fixture(scope='function')
def db(db_dsn):
    engine = create_engine(db_dsn)
    conn = engine.connect()
    yield conn
    conn.close()


@pytest.mark.asyncio
@pytest.fixture
async def sa_engine(db_dsn, loop: AbstractEventLoop) -> Engine:
    return await aiopg.sa.create_engine(db_dsn)


@pytest.fixture(scope='function')
def fill_db(db, do_truncate_db):
    def _wrapper(dump):
        do_truncate_db()

        from jsearch.common.tables import TABLES
        for table in TABLES:
            records = dump.get(table.name)
            if records:
                db.execute(table.insert(), records)
                logger.info('Fill table', extra={'table_name': table.name})
            else:
                logger.info('Table has empty data', extra={'table_name': table.name})

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


@pytest.fixture(scope='function', autouse=True)
def mock_settings(mocker, db_dsn):
    mocker.patch('jsearch.settings.JSEARCH_MAIN_DB', db_dsn)
