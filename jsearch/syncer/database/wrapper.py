import aiopg
from aiopg.sa import Engine
from psycopg2.extras import DictCursor
from typing import Optional

from jsearch.common.db import DbActionsMixin

TIMEOUT = 60


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
