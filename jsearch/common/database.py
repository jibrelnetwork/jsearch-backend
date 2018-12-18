import logging
import re
from typing import Dict

import aiopg
import psycopg2
from aiopg import sa
from aiopg.sa import create_engine as async_create_engine, Engine as AsyncEngine
from psycopg2.extras import DictCursor
from sqlalchemy import and_, false, or_, create_engine as sync_create_engine, true
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine.base import Connection, Engine as SyncEngine
from sqlalchemy.pool import NullPool
from sqlalchemy.sql import select

from jsearch import settings
from jsearch.common.tables import transactions_t, receipts_t, logs_t, token_holders_t
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


class DBWrapper:
    connection_string: str
    params: Dict[any, any]
    conn: Connection

    def __init__(self, connection_string, **params):
        self.connection_string = connection_string
        self.params = params
        self.conn = None

    async def connect(self):
        self.conn = await aiopg.connect(
            self.connection_string)

    def disconnect(self):
        self.conn.close()


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


class MainDB(DBWrapper):
    """
    jSearch Main db wrapper
    """
    engine: AsyncEngine

    async def connect(self):
        self.engine = await async_create_engine(self.connection_string, minsize=1, maxsize=MAIN_DB_POOL_SIZE)

    async def disconnect(self):
        self.engine.close()
        await self.engine.wait_closed()

    async def get_latest_sequence_synced_block_number(self, blocks_range):
        """
        Get latest block writed in main DB during sequence sync
        """
        if blocks_range[1] is None:
            condition = 'number >= %s'
            params = (blocks_range[0],)
        else:
            condition = 'number BETWEEN %s AND %s'
            params = blocks_range

        q = f"""
            SELECT l.number + 1 as start
                FROM (SELECT * FROM blocks WHERE {condition}) as l
            LEFT OUTER JOIN blocks as r ON l.number + 1 = r.number
            WHERE r.number IS NULL order by start;
        """

        async with self.engine.acquire() as conn:
            res = await conn.execute(q, params)
            rows = await res.fetchall()
            row = rows[0] if len(rows) > 0 else None
        return row['start'] - 1 if row else None

    async def get_contact_creation_code(self, address):
        q = select([transactions_t.c.input]).select_from(
            transactions_t.join(receipts_t, and_(receipts_t.c.transaction_hash == transactions_t.c.hash,
                                                 receipts_t.c.contract_address == address)))
        async with self.engine.acquire() as conn:
            res = await conn.execute(q)
            row = await res.fetchone()
        return row['input']


class MainDBSync(DBWrapperSync):
    engine: SyncEngine

    def connect(self):
        self.engine = sync_create_engine(self.connection_string, poolclass=NullPool)
        self.conn = self.engine.connect()

    def disconnect(self):
        self.conn.close()

    def update_logs(self, logs, conn=None):
        conn = self.conn or conn
        for record in logs:
            self.update_log(record, conn)

    def update_log(self, record, conn=None):
        conn = self.conn or conn

        query = logs_t.update(). \
            where(and_(logs_t.c.transaction_hash == record['transaction_hash'],
                       logs_t.c.log_index == record['log_index'])). \
            values(**record)
        conn.execute(query)

    @as_dicts
    def get_transaction_logs(self, tx_hash):
        q = select([logs_t]).where(logs_t.c.transaction_hash == tx_hash)
        return self.conn.execute(q).fetchall()

    @as_dicts
    def get_logs_to_process_events(self, limit=1000):
        unprocessed_blocks_query = select(
            columns=[logs_t.c.block_number],
            whereclause=logs_t.c.is_processed == false(),
            distinct=True
        ) \
            .order_by(logs_t.c.block_number.desc()) \
            .limit(limit)
        unprocessed_blocks = [row[0] for row in self.conn.execute(unprocessed_blocks_query).fetchall()]

        query = select(
            columns=[logs_t],
            whereclause=and_(
                or_(logs_t.c.is_processed == false(), logs_t.c.is_processed == false()),
                logs_t.c.block_number.in_(unprocessed_blocks)
            )
        ) \
            .order_by(logs_t.c.block_number.desc()) \
            .limit(limit)
        return self.conn.execute(query).fetchall()

    @as_dicts
    def get_logs_to_process_operations(self, limit=1000):
        unprocessed_blocks_query = select(
            columns=[logs_t.c.block_number],
            whereclause=and_(
                logs_t.c.is_token_transfer == true(),
                logs_t.c.is_transfer_processed == false()
            ),
            distinct=True
        ) \
            .order_by(logs_t.c.block_number.desc()) \
            .limit(limit)
        unprocessed_blocks = [row[0] for row in self.conn.execute(unprocessed_blocks_query).fetchall()]

        query = select(
            columns=[logs_t],
            whereclause=and_(
                logs_t.c.is_token_transfer == true(),
                logs_t.c.is_transfer_processed == false(),
                logs_t.c.block_number.in_(unprocessed_blocks),
            )
        ) \
            .order_by(logs_t.c.block_number.desc()) \
            .limit(limit)
        return self.conn.execute(query).fetchall()

    def get_contract_transactions(self, address):
        q = select([transactions_t]).where(transactions_t.c.to == address)
        return self.conn.execute(q).fetchall()

    def update_token_holder_balance(self, token_address, account_address, balance):
        insert_query = insert(token_holders_t).values(
            token_address=token_address,
            account_address=account_address,
            balance=balance)
        do_update_query = insert_query.on_conflict_do_update(
            index_elements=['token_address', 'account_address'],
            set_=dict(balance=balance)
        )
        self.conn.execute(do_update_query)


first_cap_re = re.compile('(.)([A-Z][a-z]+)')
all_cap_re = re.compile('([a-z0-9])([A-Z])')


def case_convert(name):
    s1 = first_cap_re.sub(r'\1_\2', name)
    return all_cap_re.sub(r'\1_\2', s1).lower()


def dict_keys_case_convert(d):
    return {case_convert(k): v for k, v in d.items()}


def get_main_db():
    db = MainDBSync(settings.JSEARCH_MAIN_DB)
    return db


def get_engine():
    db = sa.create_engine(settings.JSEARCH_MAIN_DB)
    db.connect()
    return db


def hex_vals_to_int(d, keys):
    for k in keys:
        d[k] = int(d[k], 16)
    return d
