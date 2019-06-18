import logging
from typing import Optional

import backoff
import psycopg2
from psycopg2.extras import DictCursor
from sqlalchemy import and_, create_engine as sync_create_engine
from sqlalchemy.engine.base import Engine as SyncEngine
from sqlalchemy.pool import NullPool
from sqlalchemy.sql import select

from jsearch import settings
from jsearch.common.tables import transactions_t, logs_t, blocks_t
from jsearch.common.utils import as_dicts

MAIN_DB_POOL_SIZE = 22

logger = logging.getLogger(__name__)


class DatabaseError(Exception):
    """
    Any problem with database operation
    """


class ConnectionError(DatabaseError):
    """
    Any problem with database connection
    """


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

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

        if exc_type:
            return False


class MainDBSync(DBWrapperSync):
    engine: SyncEngine

    def connect(self):
        self.engine = sync_create_engine(self.connection_string, poolclass=NullPool)
        self.conn = self.engine.connect()

    def disconnect(self):
        self.conn.close()

    @as_dicts
    @backoff.on_exception(backoff.fibo, max_tries=10, exception=psycopg2.OperationalError)
    def get_blocks(self, hashes, offset: Optional[int] = None, limit: Optional[int] = None):
        query = blocks_t.select().where(blocks_t.c.hash.in_(hashes))

        if limit:
            query = query.limit(limit)

        if offset:
            query = query.offset(offset)

        return self.conn.execute(query)

    @backoff.on_exception(backoff.fibo, max_tries=10, exception=psycopg2.OperationalError)
    def update_log(self, record, conn=None):
        conn = self.conn or conn

        query = logs_t.update(). \
            where(and_(logs_t.c.transaction_hash == record['transaction_hash'],
                       logs_t.c.block_hash == record['block_hash'],
                       logs_t.c.log_index == record['log_index'])). \
            values(**record)
        conn.execute(query)

    @as_dicts
    @backoff.on_exception(backoff.fibo, max_tries=10, exception=psycopg2.OperationalError)
    def get_transaction_logs(self, tx_hash):
        q = select([logs_t]).where(logs_t.c.transaction_hash == tx_hash)
        return self.conn.execute(q).fetchall()

    def get_contract_transactions(self, address):
        q = select([transactions_t]).where(transactions_t.c.to == address)
        return self.conn.execute(q).fetchall()


def get_main_db():
    db = MainDBSync(settings.JSEARCH_MAIN_DB)
    return db
