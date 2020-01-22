import contextlib

import backoff
import psycopg2
from aiopg.sa import Engine, SAConnection
from aiopg.sa.result import ResultProxy
from sqlalchemy.orm import Query
from typing import Any, Union, AsyncGenerator, Dict, List, Optional


@contextlib.asynccontextmanager
async def acquire_connection(engine: Union[Engine, SAConnection]) -> AsyncGenerator[SAConnection, None]:
    if isinstance(engine, Engine):
        connection = await engine.acquire()
    else:
        connection = engine

    yield connection

    if isinstance(engine, Engine):
        engine.release(connection)


@backoff.on_exception(backoff.fibo, max_tries=3, exception=psycopg2.OperationalError)
async def execute(pool: Union[Engine, SAConnection], query: Union[Query, str], *params: Any):
    async with acquire_connection(pool) as connection:
        return await connection.execute(query, params)


@backoff.on_exception(backoff.fibo, max_tries=3, exception=psycopg2.OperationalError)
async def fetch_all(pool: Union[Engine, SAConnection], query: Union[Query, str], *params: Any) -> List[Dict[str, Any]]:
    async with acquire_connection(pool) as connection:
        cursor = await connection.execute(query, params)
        results = await cursor.fetchall()
    return [dict(item) for item in results]


@backoff.on_exception(backoff.fibo, max_tries=3, exception=psycopg2.OperationalError)
async def fetch_one(
        pool: Union[Engine, SAConnection],
        query: Union[Query, str],
        *params: Any
) -> Optional[Dict[str, Any]]:
    async with acquire_connection(pool) as connection:
        connection = await connection.execute(query, params)
        result = await connection.fetchone()

    return dict(result) if result else None


@backoff.on_exception(backoff.fibo, max_tries=3, exception=psycopg2.OperationalError)
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
