import logging
import os

import dsnparse
import pytest
from aiopg.sa import Engine
from pathlib import Path
from sqlalchemy import MetaData
from sqlalchemy import create_engine
from typing import Optional, Callable, Any

from .utils import apply_dump, load_json_dump, truncate

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

SQL_FOLDER = Path(__file__).parent / "sql"
DUMPS_FOLDER = Path(__file__).parent / "dumps" / "raw_db"


def setup_database(connection_string):
    dsn = dsnparse.parse(connection_string)
    engine = create_engine(
        f'postgres://{dsn.netloc}/postgres',
        execution_options={'isolation_level': 'AUTOCOMMIT'}
    )
    engine.execute(f'DROP DATABASE IF EXISTS "{dsn.dbname}";')
    engine.execute(f'CREATE DATABASE "{dsn.dbname}";')

    engine = create_engine(connection_string)
    with engine.connect() as db:
        sql = SQL_FOLDER / "raw_db_initial.sql"
        db.execute(sql.read_text())


def teardown_database(connection_string):
    dsn = dsnparse.parse(connection_string)
    engine = create_engine(
        f'postgres://{dsn.netloc}/postgres',
        execution_options={'isolation_level': 'AUTOCOMMIT'}
    )
    engine.execute(
        f"""
        SELECT pg_terminate_backend(pg_stat_activity.pid)
        FROM pg_stat_activity
        WHERE pg_stat_activity.datname = '{dsn.dbname}'
            AND pid <> pg_backend_pid()
        """
    )
    engine.execute(f'DROP DATABASE IF EXISTS "{dsn.dbname}";')


@pytest.fixture(scope="session")
def raw_db_dsn():
    return os.environ['JSEARCH_RAW_DB']


@pytest.fixture(scope="session", autouse=True)
def raw_db_create_tables(raw_db_dsn):
    setup_database(connection_string=raw_db_dsn)
    yield
    teardown_database(raw_db_dsn)


@pytest.fixture(scope='session', autouse=True)
def raw_db_meta(raw_db):
    meta = MetaData()
    meta.reflect(bind=raw_db)

    return meta


@pytest.fixture(scope="session")
def raw_db(raw_db_dsn):
    engine = create_engine(raw_db_dsn)
    conn = engine.connect()
    yield conn
    conn.close()


@pytest.fixture(scope="session")
async def raw_db_wrapper(raw_db_dsn):
    from jsearch.syncer.database import RawDB

    raw_db_wrapper = RawDB(raw_db_dsn)

    await raw_db_wrapper.connect()
    yield raw_db_wrapper
    await raw_db_wrapper.disconnect()


@pytest.fixture(scope="function", autouse=True)
def truncate_raw_db(raw_db: Engine, raw_db_meta: MetaData):
    yield
    return truncate(raw_db, raw_db_meta)


@pytest.fixture(scope="module")
def load_blocks(
        raw_db: Engine,
        raw_db_meta: MetaData,
        raw_db_create_tables: None
) -> Callable[..., Any]:
    def load(block_start, block_end: Optional[int] = None) -> None:
        block_end = block_end or block_start
        dump: Path = DUMPS_FOLDER / f'{block_start}-{block_end}.json'

        tables = load_json_dump(dump)
        apply_dump(raw_db, tables)

    yield load

    truncate(raw_db, raw_db_meta)
