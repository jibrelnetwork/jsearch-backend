import aiopg
import psycopg2
from psycopg2.extras import DictCursor


class DatabaseError(Exception):
    """
    Any problem with database operation
    """


class ConnectionError(DatabaseError):
    """
    Any problem with database connection
    """


class DBWrapper:

    def __init__(self, connection_string, **params):
        self.connection_string = connection_string
        self.params = params
        self.pool = None

    async def connect(self):
        self.pool = await aiopg.create_pool(
            self.connection_string, cursor_factory=DictCursor)

    def disconnect(self):
        self.pool.close()

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, *exc_info):
        self.disconnect()
        if any(exc_info):
            return False


class DBWrapperSync:

    def __init__(self, connection_string, **params):
        self.connection_string = connection_string
        self.params = params
        self.conn = None

    def connect(self):
        self.conn = psycopg2.connect(self.connection_string, cursor_factory=DictCursor)

    def disconnect(self):
        self.conn.close()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *exc_info):
        self.disconnect()
        if any(exc_info):
            return False
