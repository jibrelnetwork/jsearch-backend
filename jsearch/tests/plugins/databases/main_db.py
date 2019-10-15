import logging
import os
from asyncio import AbstractEventLoop

import aiopg
import asyncpg
import pytest
from aiopg.sa import Engine
from sqlalchemy import create_engine

from jsearch.api.storage import Storage
from manage import GooseWrapper, MIGRATIONS_FOLDER

logger = logging.getLogger(__name__)


def get_db_name(db_dsn: str) -> str:
    return db_dsn.split('/')[-1]


def reset_postgres_activities(db_dsn: str) -> None:
    engine = create_engine(db_dsn)
    db_name = get_db_name(db_dsn)
    with engine.connect() as db:
        db.execute(
            f"""
            SELECT pg_terminate_backend(pg_stat_activity.pid)
            FROM pg_stat_activity
            WHERE pg_stat_activity.datname = '{db_name}'
                AND pid <> pg_backend_pid();
            """
        )


@pytest.fixture(scope="session")
def db_dsn():
    return os.environ['JSEARCH_MAIN_DB']


@pytest.fixture(scope="session")
def goose(db_dsn):
    return GooseWrapper(db_dsn, MIGRATIONS_FOLDER)


@pytest.fixture(scope='session', autouse=True)
def apply_migrations(goose, db_dsn, pytestconfig):
    goose.up()
    yield
    reset_postgres_activities(db_dsn)
    goose.down()


@pytest.fixture(scope='function', autouse=True)
def truncate_tables(db):
    yield
    
    from jsearch.common.tables import TABLES

    tables = ",".join([table.name for table in TABLES])
    db.execute(f"TRUNCATE {tables};")


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


@pytest.mark.asyncio
@pytest.fixture()
async def storage(db_dsn: str, loop: AbstractEventLoop) -> Storage:
    db_pool = await asyncpg.create_pool(dsn=db_dsn)
    storage = Storage(db_pool)

    yield storage

    await db_pool.close()
