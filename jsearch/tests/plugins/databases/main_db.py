import logging
import os
from asyncio import AbstractEventLoop

import aiopg
import pytest
from aiopg.sa import Engine
from pathlib import Path
from sqlalchemy import MetaData
from sqlalchemy import create_engine
from typing import Optional, Callable, Any

from jsearch.api.storage import Storage
from manage import GooseWrapper, MIGRATIONS_FOLDER
from .utils import load_json_dump, truncate, TableDescription

logger = logging.getLogger(__name__)

DUMPS_FOLDER = Path(__file__).parent / "dumps" / "main_db"


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
def apply_migrations(goose, db_dsn):
    goose.up()
    yield
    reset_postgres_activities(db_dsn)
    goose.down()


@pytest.fixture(scope='session')
def main_db_meta(db, apply_migrations):
    meta = MetaData()
    meta.reflect(bind=db)

    return meta


@pytest.fixture(scope='session')
def db(db_dsn):
    engine = create_engine(db_dsn)
    conn = engine.connect()
    try:
        yield conn
    finally:
        conn.close()


@pytest.mark.asyncio
@pytest.fixture
async def sa_engine(db_dsn, loop: AbstractEventLoop) -> Engine:
    return await aiopg.sa.create_engine(db_dsn)


@pytest.fixture(scope="module")
async def main_db_wrapper(db_dsn):
    from jsearch.syncer.database import MainDB

    main_db = MainDB(db_dsn)
    await main_db.connect()

    try:
        yield main_db
    finally:
        await main_db.disconnect()


@pytest.mark.asyncio
@pytest.fixture()
async def storage(db_dsn: str, sa_engine, loop: AbstractEventLoop) -> 'Storage':
    storage = Storage(sa_engine)
    yield storage


@pytest.fixture(autouse=True)
def truncate_main_db(db: Engine, main_db_meta: MetaData):
    yield
    return truncate(db, main_db_meta, exclude='goose_db_version')


@pytest.fixture()
def get_main_db_dump() -> Callable[..., Any]:
    def load(block_start: int, table: str, block_end: Optional[int] = None) -> TableDescription:
        block_end = block_end or block_start
        dump: Path = DUMPS_FOLDER / f'{block_start}-{block_end}.json'

        dumps = load_json_dump(dump)
        for table_dump in dumps:
            if table_dump.name == table:
                return table_dump
        else:
            raise ValueError(f'Dump is missed {table}')

    yield load
