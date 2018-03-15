import asyncio
import logging
import re
import time
import json

import asyncpg
from asyncpgsa import pg
import sqlalchemy as sa

from jsearch.common.tables import *


logger = logging.getLogger(__name__)


class DatabaseError(Exception):
    """
    Any problem with database operation
    """


class ConnectionError(DatabaseError):
    """
    Any problem with database connection
    """


class LoggingConnection(asyncpg.connection.Connection):
    """
    Connection subclass with query logging
    """

    async def _execute(self, query, args, limit, timeout, return_status=False):
        start_time = time.monotonic()
        res = await super()._execute(query, args, limit, timeout, return_status)
        query_time = time.monotonic() - start_time
        logger.debug("%s params %s [%s]", query, args, query_time)
        return res

    async def execute(self, query: str, *args, timeout: float=None) -> str:
        start_time = time.monotonic()
        res = await super().execute(query, args, timeout)
        query_time = time.monotonic() - start_time
        logger.debug("%s params %s [%s]", query, args, query_time)
        return res


class DBWrapper:

    def __init__(self, connection_string, **params):
        self.connection_string = connection_string
        self.params = params
        self.conn = None

    async def connect(self):
        self.conn = await asyncpg.connect(
            self.connection_string, connection_class=LoggingConnection)

    async def disconnect(self):
        await self.conn.close()


class RawDB(DBWrapper):
    """
    jSearch RAW db wrapper
    """
    async def get_blocks_to_sync(self, start_block_num=0, chunk_size=10):
        q = """SELECT * FROM headers WHERE block_number BETWEEN $1 AND $2"""
        end_num = start_block_num + chunk_size
        rows = await self.conn.fetch(q, start_block_num, end_num)
        return rows

    async def get_header_by_hash(self, block_hash):
        q = """SELECT * FROM headers WHERE block_hash=$1"""
        row = await self.conn.fetchrow(q, block_hash)
        return row

    async def get_block_accounts(self, block_hash):
        q = """SELECT * FROM accounts WHERE block_hash=$1"""
        rows = await self.conn.fetch(q, block_hash)
        return rows

    async def get_block_body(self, block_hash):
        q = """SELECT * FROM bodies WHERE block_hash=$1"""
        rows = await self.conn.fetchrow(q, block_hash)
        return rows

    async def get_block_receipts(self, block_hash):
        q = """SELECT * FROM receipts WHERE block_hash=$1"""
        rows = await self.conn.fetchrow(q, block_hash)
        return rows


class MainDB(DBWrapper):
    """
    jSearch Main db wrapper
    """
    async def connect(self):
        await pg.init(self.connection_string)

    async def disconnect(self):
        pass

    async def get_latest_sequence_synced_block_number(self):
        """
        Get latest block writed in main DB during sequence sync
        """
        q = """SELECT number FROM blocks
                WHERE number=(SELECT max(number) FROM BLOCKS WHERE is_sequence_sync=true)
                AND is_sequence_sync=true"""
        row = await pg.fetchrow(q)
        return row['number'] if row else None

    async def write_block(self, header, uncles, transactions, receipts, accounts):
        """
        Write block and all related items in main database
        """
        print('H:', header)
        print('U:', uncles)
        print('A:', accounts)
        print('T:', transactions)
        print('R:', receipts)
        block_number = header['block_number']
        block_hash = header['block_hash']

        async with pg.transaction() as conn:
            await self.insert_header(conn, header)
            await self.insert_uncles(conn, block_number, block_hash, uncles)
            await self.insert_transactions(conn, block_number, block_hash, transactions)
            await self.insert_receipts(conn, block_number, block_hash, receipts)
            await self.insert_accounts(conn, block_number, block_hash, accounts)

    async def insert_header(self, conn, header):
        data = dict_keys_case_convert(json.loads(header['fields']))
        query = blocks_t.insert().values(is_sequence_sync=True, **data)
        await conn.execute(query)

    async def insert_uncles(self, conn, block_number, block_hash, uncles):
        for uncle in uncles:
            data = dict_keys_case_convert(uncle)
            query = uncles_t.insert().values(block_number=block_number,
                                             block_hash=block_hash, **data)
            await conn.execute(query)

    async def insert_transactions(self, conn, block_number, block_hash, transactions):
        for transaction in transactions:
            data = dict_keys_case_convert(transaction)
            query = transactions_t.insert().values(block_number=block_number,
                                                   block_hash=block_hash, **data)
            await conn.execute(query)

    async def insert_receipts(self, conn, block_number, block_hash, receipts):
        rdata = json.loads(receipts['fields'])['Receipts'] or []
        for receipt in rdata:
            data = dict_keys_case_convert(receipt)
            logs = data.pop('logs') or []
            query = receipts_t.insert().values(block_number=block_number,
                                               block_hash=block_hash, **data)
            await conn.execute(query)
            await self.insert_logs(conn, block_number, block_hash, logs)

    async def insert_logs(self, conn, block_number, block_hash, logs):
        for log_record in logs:
            data = dict_keys_case_convert(log_record)
            query = logs_t.insert().values(**data)
            await conn.execute(query)

    async def insert_accounts(self, conn, block_number, block_hash, accounts):
        for account in accounts:
            data = dict_keys_case_convert(json.loads(account['fields']))
            data['storage'] = None  # FIXME!!! 
            query = accounts_t.insert().values(block_number=block_number,
                                               block_hash=block_hash, **data)
            await conn.execute(query)


first_cap_re = re.compile('(.)([A-Z][a-z]+)')
all_cap_re = re.compile('([a-z0-9])([A-Z])')


def case_convert(name):
    s1 = first_cap_re.sub(r'\1_\2', name)
    return all_cap_re.sub(r'\1_\2', s1).lower()


def dict_keys_case_convert(d):
    return {case_convert(k): v for k, v in d.items()}
