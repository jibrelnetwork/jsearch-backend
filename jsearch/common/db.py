import asyncio
import contextlib
import logging
from dataclasses import dataclass
from typing import AsyncGenerator, Dict, List
from typing import Callable, Any, Union
from typing import Optional

import aiopg
import async_timeout
import backoff
import psycopg2
from aiopg.sa import Engine
from aiopg.sa import SAConnection
from aiopg.sa.result import ResultProxy
from psycopg2.extras import DictCursor
from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.sql import ClauseElement

from jsearch import settings

TIMEOUT = 60
logger = logging.getLogger(__name__)


@contextlib.asynccontextmanager
async def acquire_connection(engine: Union[Engine, SAConnection]) -> AsyncGenerator[SAConnection, None]:
    if isinstance(engine, Engine):
        connection = await engine.acquire()
    else:
        connection = engine

    try:
        yield connection
    finally:
        if isinstance(engine, Engine):
            engine.release(connection)


def query_timeout(func: Callable[..., Any]) -> Callable[..., Any]:
    timeout = settings.QUERY_TIMEOUT

    async def _wrapper(pool: Union[Engine, SAConnection], query: Union[str, ClauseElement], *params) -> Any:
        assert query is not None, "Query can't be empty"

        try:
            async with async_timeout.timeout(timeout):
                return await func(pool, query, *params)
        except asyncio.TimeoutError:
            if isinstance(query, ClauseElement):
                query = str(query.compile(dialect=dialect(), compile_kwargs={"literal_binds": True}))
            logger.error(
                'Query exceeds time limits',
                extra={
                    'timeout': timeout,
                    'query': query,
                    'params': params
                }
            )
            raise

    return _wrapper


@backoff.on_exception(backoff.fibo, max_tries=3, exception=psycopg2.OperationalError)
@query_timeout
async def execute(pool: Union[Engine, SAConnection], query: Union[ClauseElement, str], *params: Any):
    async with acquire_connection(pool) as connection:
        return await connection.execute(query, params)


@backoff.on_exception(backoff.fibo, max_tries=3, exception=psycopg2.OperationalError)
@query_timeout
async def fetch_all(
        pool: Union[Engine, SAConnection],
        query: Union[ClauseElement, str],
        *params: Any,
) -> List[Dict[str, Any]]:
    async with acquire_connection(pool) as connection:
        cursor = await connection.execute(query, params)
        results = await cursor.fetchall()
    return [dict(item) for item in results]


@backoff.on_exception(backoff.fibo, max_tries=3, exception=psycopg2.OperationalError)
@query_timeout
async def fetch_one(
        pool: Union[Engine, SAConnection],
        query: Union[ClauseElement, str],
        *params: Any
) -> Optional[Dict[str, Any]]:
    async with acquire_connection(pool) as connection:
        connection = await connection.execute(query, params)
        result = await connection.fetchone()

    return dict(result) if result else None


async def get_iterator(
        pool: Engine,
        query: ClauseElement,
        *params,
        size: int = 1
) -> AsyncGenerator[Dict[str, Any], None]:
    async with acquire_connection(pool) as connection:
        connection = await connection.execute(query, params)
        result = await connection.fetchmany(size)

        while result:
            for item in result:
                yield dict(item)

            result = await connection.fetchmany(size)
            if not result:
                break


@dataclass
class DbActionsMixin:
    engine: Engine

    async def execute(self, query: Union[str, ClauseElement], *params) -> ResultProxy:
        return await execute(self.engine, query, *params)

    async def fetch_all(self, query: Union[str, ClauseElement], *params) -> List[Dict[str, Any]]:
        return await fetch_all(self.engine, query, *params)

    async def fetch_one(self, query: Union[str, ClauseElement], *params) -> Dict[str, Any]:
        return await fetch_one(self.engine, query, *params)

    async def iterate_by(self, query: Union[str, ClauseElement], *params) -> AsyncGenerator[Dict[str, Any], None]:
        return get_iterator(self.engine, query, *params)


class DBWrapper(DbActionsMixin):
    connection_string: str

    pool_size: int
    timeout: int = TIMEOUT
    pool: Optional[Engine] = None

    def __init__(self, connection_string, **params):
        self.connection_string = connection_string
        self.params = params
        self.engine = None

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, *exc_info):
        await self.disconnect()
        if any(exc_info):
            return False

    async def connect(self):
        self.engine = await aiopg.sa.create_engine(
            self.connection_string,
            minsize=1,
            maxsize=self.pool_size,
            timeout=self.timeout,
            cursor_factory=DictCursor,
            **self.params
        )

    async def disconnect(self):
        if self.engine is not None:
            self.engine.terminate()
