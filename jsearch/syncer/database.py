import logging
import re

import aiopg
import psycopg2
from aiopg.sa import create_engine
from psycopg2.extras import DictCursor
from sqlalchemy.engine.base import Engine as SyncEngine
from sqlalchemy.pool import NullPool

from jsearch.common import contracts
from jsearch.common.tables import *

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

    def connect(self):
        self.conn = psycopg2.connect(self.connection_string, cursor_factory=DictCursor)

    def disconnect(self):
        self.conn.close()

    def get_header_by_hash(self, block_number):
        q = """SELECT * FROM headers WHERE block_number=%s"""
        with self.conn.cursor() as cur:
            cur.execute(q, [block_number])
            row = cur.fetchone()
        return row

    def get_header_by_block_number(self, block_number):
        q = """SELECT * FROM headers WHERE block_number=%s"""
        with self.conn.cursor() as cur:
            cur.execute(q, [block_number])
            row = cur.fetchone()
        return row

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

    engine: SyncEngine

    async def connect(self):
        self.engine = await create_engine(self.connection_string, minsize=1, maxsize=MAIN_DB_POOL_SIZE)

    async def disconnect(self):
        self.engine.close()
        await self.engine.wait_closed()

    async def get_latest_sequence_synced_block_number(self, blocks_range):
        """
        Get latest block writed in main DB
        """
        if blocks_range[1] is None:
            condition = 'number >= %s'
            params = (blocks_range[0],)
        else:
            condition = 'number BETWEEN %s AND %s'
            params = blocks_range
        q = """SELECT l.number + 1 as start 
                FROM (SELECT * FROM blocks WHERE {cond}) as l 
                LEFT OUTER JOIN blocks as r ON l.number + 1 = r.number
                WHERE r.number IS NULL order by start""".format(cond=condition)
        async with self.engine.acquire() as conn:
            res = await conn.execute(q, params)
            rows = await res.fetchall()
            row = rows[0] if len(rows) > 0 else None
        return row['start'] - 1 if row else None


class MainDBSync(DBWrapperSync):

    def connect(self):
        engine = sa.create_engine(self.connection_string, poolclass=NullPool)
        self.conn = engine.connect()

    def is_block_exist(self, block_number):
        q = """SELECT number from blocks WHERE number=%s"""
        row = self.conn.execute(q, [block_number]).fetchone()
        return row['number'] == block_number if row else False

    def write_block_data(self, block_data, uncles_data, transactions_data, receipts_data,
                         logs_data, accounts_data, internal_txs_data):
        """
        Insert block and all related items in main database
        """

        with self.conn.begin():
            self.insert_block(block_data)
            self.insert_uncles(uncles_data)
            self.insert_transactions(transactions_data)
            self.insert_receipts(receipts_data)
            self.insert_logs(logs_data)
            self.insert_accounts(accounts_data)
            self.insert_internal_transactions(internal_txs_data)

    def insert_block(self, block_data):
        if block_data:
            self.conn.execute(blocks_t.insert(), block_data)

    def insert_uncles(self, uncles_data):
        if uncles_data:
            self.conn.execute(uncles_t.insert(), *uncles_data)

    def insert_transactions(self, transactions_data):
        if transactions_data:
            self.conn.execute(transactions_t.insert(), *transactions_data)

    def insert_receipts(self, receipts_data):
        if receipts_data:
            self.conn.execute(receipts_t.insert(), *receipts_data)

    def insert_logs(self, logs_data):
        if logs_data:
            self.conn.execute(logs_t.insert(), *logs_data)

    def insert_accounts(self, accounts):
        if accounts:
            self.conn.execute(accounts_t.insert(), *accounts)

    def insert_internal_transactions(self, internal_transactions):
        if internal_transactions:
            self.conn.execute(internal_transactions_t.insert(), *internal_transactions)


first_cap_re = re.compile('(.)([A-Z][a-z]+)')
all_cap_re = re.compile('([a-z0-9])([A-Z])')


def case_convert(name):
    s1 = first_cap_re.sub(r'\1_\2', name)
    return all_cap_re.sub(r'\1_\2', s1).lower()


def dict_keys_case_convert(d):
    return {case_convert(k): v for k, v in d.items()}


def hex_vals_to_int(d, keys):
    for k in keys:
        d[k] = int(d[k], 16)
