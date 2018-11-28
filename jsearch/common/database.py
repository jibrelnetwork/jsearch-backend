import logging
import re
from typing import Dict

import aiopg
import psycopg2
from aiopg import sa
from aiopg.sa import create_engine as async_create_engine, Engine as AsyncEngine
from psycopg2.extras import DictCursor
from sqlalchemy import and_, false, null, or_, create_engine as sync_create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine.base import Connection, Engine as SyncEngine
from sqlalchemy.pool import NullPool
from sqlalchemy.sql import select

from jsearch import settings
from jsearch.common import contracts
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


class RawDB(DBWrapper):
    """
    jSearch RAW db wrapper
    """

    async def get_blocks_to_sync(self, start_block_num=0, end_block_num=None):
        q = """SELECT block_number FROM headers WHERE block_number BETWEEN %s AND %s"""
        async with self.conn.cursor() as cur:
            await cur.execute(q, [start_block_num, end_block_num])
            rows = await cur.fetchall()
        return rows


class RawDBSync(DBWrapperSync):

    def get_block_accounts(self, block_number):
        q = """SELECT * FROM accounts WHERE block_number=%s"""
        with self.conn.cursor() as cur:
            cur.execute(q, [block_number])
            rows = cur.fetchall()
        return rows

    def get_block_body(self, block_number):
        q = """SELECT * FROM bodies WHERE block_number=%s"""
        with self.conn.cursor() as cur:
            cur.execute(q, [block_number])
            row = cur.fetchone()
        return row

    def get_block_receipts(self, block_number):
        q = """SELECT * FROM receipts WHERE block_number=%s"""
        with self.conn.cursor() as cur:
            cur.execute(q, [block_number])
            row = cur.fetchone()
        return row

    def get_reward(self, block_number):
        q = """SELECT * FROM rewards WHERE block_number=%s"""
        with self.conn.cursor() as cur:
            cur.execute(q, [block_number])
            rows = cur.fetchall()
        if len(rows) > 1:
            for r in rows:
                if r['address'] != contracts.NULL_ADDRESS:
                    return r
        elif len(rows) == 1:
            return rows[0]
        else:
            return None

    def get_internal_transactions(self, block_number):
        q = """SELECT * FROM internal_transactions WHERE block_number=%s"""
        with self.conn.cursor() as cur:
            cur.execute(q, [block_number])
            rows = cur.fetchall()
        return rows


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

    def update_logs(self, conn, logs):
        for rec in logs:
            query = logs_t.update(). \
                where(and_(logs_t.c.transaction_hash == rec['transaction_hash'],
                           logs_t.c.log_index == rec['log_index'])). \
                values(**rec)
            conn.execute(query)

    @as_dicts
    def get_transaction_logs(self, tx_hash):
        q = select([logs_t]).where(logs_t.c.transaction_hash == tx_hash)
        return self.conn.execute(q).fetchall()

    @as_dicts
    def get_logs_for_post_processing(self, limit=1000):
        query = select([logs_t]) \
            .where(or_(logs_t.c.is_processed == false(),
                       logs_t.c.is_processed == null())) \
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
