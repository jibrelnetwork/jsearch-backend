import backoff
import psycopg2
from aiopg.sa import create_engine as async_create_engine, Engine
from psycopg2.extras import DictCursor

TIMEOUT = 60


class DBWrapper:
    engine: Engine
    pool_size: int
    timeout: int = TIMEOUT

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
        self.engine = await async_create_engine(
            self.connection_string,
            minsize=1,
            maxsize=self.pool_size,
            timeout=self.timeout,
            cursor_factory=DictCursor,
            **self.params
        )

    async def disconnect(self):
        if self.engine is not None:
            self.engine.close()
            await self.engine.wait_closed()

    @backoff.on_exception(backoff.fibo, max_tries=10, exception=psycopg2.OperationalError)
    async def execute(self, query, *params):
        async with self.engine.acquire() as connection:
            return await connection.execute(query, params)

    @backoff.on_exception(backoff.fibo, max_tries=10, exception=psycopg2.OperationalError)
    async def fetch_all(self, query, *params):
        async with self.engine.acquire() as connection:
            cursor = await connection.execute(query, params)
            return await cursor.fetchall()

    @backoff.on_exception(backoff.fibo, max_tries=10, exception=psycopg2.OperationalError)
    async def fetch_one(self, query, *params):
        async with self.engine.acquire() as connection:
            cursor = await connection.execute(query, params)
            return await cursor.fetchone()
