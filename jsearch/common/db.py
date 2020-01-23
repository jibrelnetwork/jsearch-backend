import asyncio
import contextlib
import logging
from typing import AsyncGenerator, Dict, List, Optional
from typing import Callable, Any, Union

import async_timeout
import backoff
import psycopg2
from aiopg.sa import SAConnection, Engine
from aiopg.sa.result import ResultProxy
from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.orm import Query

from jsearch import settings

logger = logging.getLogger(__name__)


@contextlib.asynccontextmanager
async def acquire_connection(engine: Union[Engine, SAConnection]) -> AsyncGenerator[SAConnection, None]:
    if isinstance(engine, Engine):
        connection = await engine.acquire()
    else:
        connection = engine

    yield connection

    if isinstance(engine, Engine):
        engine.release(connection)


def query_timeout(func: Callable[..., Any]) -> Callable[..., Any]:
    timeout = settings.QUERY_TIMEOUT

    async def _wrapper(pool: Union[Engine, SAConnection], query: Union[str, Query], *params) -> Any:
        assert query is not None, "Query can't be empty"

        try:
            async with async_timeout.timeout(timeout):
                return await func(pool, query, *params)
        except asyncio.TimeoutError:
            if isinstance(query, Query):
                query = str(query.compile(dialect=dialect(), compile_kwargs={"literal_binds": True}))
            logging.error(
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
async def execute(pool: Union[Engine, SAConnection], query: Union[Query, str], *params: Any):
    async with acquire_connection(pool) as connection:
        return await connection.execute(query, params)


@backoff.on_exception(backoff.fibo, max_tries=3, exception=psycopg2.OperationalError)
@query_timeout
async def fetch_all(pool: Union[Engine, SAConnection], query: Union[Query, str], *params: Any) -> List[Dict[str, Any]]:
    async with acquire_connection(pool) as connection:
        cursor = await connection.execute(query, params)
        results = await cursor.fetchall()
    return [dict(item) for item in results]


@backoff.on_exception(backoff.fibo, max_tries=3, exception=psycopg2.OperationalError)
@query_timeout
async def fetch_one(
        pool: Union[Engine, SAConnection],
        query: Union[Query, str],
        *params: Any
) -> Optional[Dict[str, Any]]:
    async with acquire_connection(pool) as connection:
        connection = await connection.execute(query, params)
        result = await connection.fetchone()

    return dict(result) if result else None


async def get_iterator(
        pool: Engine,
        query: Query,
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


class DbActionsMixin:
    engine: Engine

    async def execute(self, query: Union[str, Query], *params) -> ResultProxy:
        return await execute(self.engine, query, *params)

    async def fetch_all(self, query: Union[str, Query], *params) -> List[Dict[str, Any]]:
        return await fetch_all(self.engine, query, *params)

    async def fetch_one(self, query: Union[str, Query], *params) -> Dict[str, Any]:
        return await fetch_one(self.engine, query, *params)

    async def iterate_by(self, query: Union[str, Query], *params) -> AsyncGenerator[Dict[str, Any], None]:
        return get_iterator(self.engine, query, *params)
