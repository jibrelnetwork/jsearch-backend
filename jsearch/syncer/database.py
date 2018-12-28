import logging
from typing import List

import aiopg
import psycopg2
from aiopg.sa import create_engine as async_create_engine
from psycopg2.extras import DictCursor
from sqlalchemy import create_engine as sync_create_engine, and_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine.base import Engine as SyncEngine
from sqlalchemy.pool import NullPool

from jsearch.common import contracts
from jsearch.common.tables import (
    accounts_base_t,
    accounts_state_t,
    blocks_t,
    internal_transactions_t,
    logs_t,
    receipts_t,
    transactions_t,
    uncles_t,
    reorgs_t,
)
from jsearch.typing import Log

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
        self.pool = None

    async def connect(self):
        self.pool = await aiopg.create_pool(
            self.connection_string, cursor_factory=DictCursor)

    def disconnect(self):
        self.pool.close()


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
        q = """SELECT block_hash, block_number FROM headers WHERE block_number BETWEEN %s AND %s"""
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(q, [start_block_num, end_block_num])
                rows = await cur.fetchall()
                cur.close()
        return rows

    async def get_missed_blocks(self, blocks_numbers):
        if not blocks_numbers:
            return []
        q = """SELECT block_hash, block_number
                FROM headers WHERE block_number IN ({})""".format(','.join('%s' for _ in blocks_numbers))
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(q, blocks_numbers)
                rows = await cur.fetchall()
                cur.close()
        return rows

    async def get_latest_available_block_number(self):
        q = """SELECT max(block_number) as max_block from bodies"""
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(q)
                row = await cur.fetchone()
                cur.close()
        return row['max_block'] or None

    async def get_reorgs_from(self, reorg_from_num, limit):
        q = """SELECT * FROM reorgs where id > %s ORDER BY id LIMIT %s"""
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(q, [reorg_from_num, limit])
                rows = await cur.fetchall()
                cur.close()
        return rows


class RawDBSync(DBWrapperSync):

    def connect(self):
        self.conn = psycopg2.connect(self.connection_string, cursor_factory=DictCursor)

    def disconnect(self):
        self.conn.close()

    def get_header_by_hash(self, block_number):
        q = """SELECT * FROM headers WHERE block_hash=%s"""
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

    def get_block_accounts(self, block_hash):
        q = """SELECT * FROM accounts WHERE block_hash=%s"""
        with self.conn.cursor() as cur:
            cur.execute(q, [block_hash])
            rows = cur.fetchall()
        return rows

    def get_block_body(self, block_hash):
        q = """SELECT * FROM bodies WHERE block_hash=%s"""
        with self.conn.cursor() as cur:
            cur.execute(q, [block_hash])
            row = cur.fetchone()
        return row

    def get_block_receipts(self, block_hash):
        q = """SELECT * FROM receipts WHERE block_hash=%s"""
        with self.conn.cursor() as cur:
            cur.execute(q, [block_hash])
            row = cur.fetchone()
        return row

    def get_reward(self, block_hash):
        q = """SELECT * FROM rewards WHERE block_hash=%s"""
        with self.conn.cursor() as cur:
            cur.execute(q, [block_hash])
            rows = cur.fetchall()
        if len(rows) > 1:
            for r in rows:
                if r['address'] != contracts.NULL_ADDRESS:
                    return r
        elif len(rows) == 1:
            return rows[0]
        else:
            return None

    def get_internal_transactions(self, block_hash):
        q = """SELECT * FROM internal_transactions WHERE block_hash=%s"""
        with self.conn.cursor() as cur:
            cur.execute(q, [block_hash])
            rows = cur.fetchall()
        return rows


class MainDB(DBWrapper):
    """
    jSearch Main db wrapper
    """

    engine: SyncEngine

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()
        if any((exc_type, exc_val, exc_tb)):
            return False

    async def connect(self):
        self.engine = await async_create_engine(self.connection_string, minsize=1, maxsize=MAIN_DB_POOL_SIZE)

    async def disconnect(self):
        self.engine.close()
        await self.engine.wait_closed()

    async def get_latest_synced_block_number(self, blocks_range):
        """
        Get latest block writed in main DB
        """
        if blocks_range[1] is None:
            condition = 'number >= %s'
            params = (blocks_range[0],)
        else:
            condition = 'number BETWEEN %s AND %s'
            params = blocks_range

        q = """SELECT max(number) as max_number
                FROM blocks
                WHERE is_forked=false AND {cond}""".format(cond=condition)
        async with self.engine.acquire() as conn:
            res = await conn.execute(q, params)
            row = await res.fetchone()
            return row['max_number']

    async def get_missed_blocks_numbers(self, limit: int):
        q = """SELECT l.number + 1 as start
                FROM (SELECT * FROM blocks WHERE is_forked=false) as l
                LEFT OUTER JOIN blocks as r ON l.number + 1 = r.number
                WHERE r.number IS NULL order by start limit %s"""
        async with self.engine.acquire() as conn:
            res = await conn.execute(q, limit)
            rows = await res.fetchall()
            if len(rows) < limit:
                # here last num is not missed, just not synced, remove them
                rows = rows[:-1]
            return [r['start'] for r in rows]

    async def apply_reorg(self, reorg):
        reorg = dict(reorg)

        update_block_q = blocks_t.update() \
            .values(is_forked=not reorg['reinserted']) \
            .where(blocks_t.c.hash == reorg['block_hash']) \
            .returning(blocks_t.c.hash)

        update_txs_q = transactions_t.update() \
            .values(is_forked=not reorg['reinserted']) \
            .where(blocks_t.c.hash == reorg['block_hash'])

        update_receipts_q = receipts_t.update() \
            .values(is_forked=not reorg['reinserted']) \
            .where(blocks_t.c.hash == reorg['block_hash'])

        update_logs_q = logs_t.update() \
            .values(is_forked=not reorg['reinserted']) \
            .where(blocks_t.c.hash == reorg['block_hash'])

        update_internal_transactions_q = internal_transactions_t.update() \
            .values(is_forked=not reorg['reinserted']) \
            .where(blocks_t.c.hash == reorg['block_hash'])

        update_accounts_state_q = accounts_state_t.update() \
            .values(is_forked=not reorg['reinserted']) \
            .where(blocks_t.c.hash == reorg['block_hash'])

        update_uncles_q = uncles_t.update() \
            .values(is_forked=not reorg['reinserted']) \
            .where(blocks_t.c.hash == reorg['block_hash'])

        reorg.pop('header')
        add_reorg_q = reorgs_t.insert().values(**reorg)
        async with self.engine.acquire() as conn:
            async with conn.begin() as tx:
                res = await conn.execute(update_block_q)
                rows = await res.fetchall()
                if len(rows) == 0:
                    # no updates, block si not synced - aborting reorg
                    logger.debug('Aborting reorg for block %s %s', reorg['block_number'], reorg['block_hash'])
                    await tx.rollback()
                    return False
                await conn.execute(update_txs_q)
                await conn.execute(update_receipts_q)
                await conn.execute(update_logs_q)
                await conn.execute(update_internal_transactions_q)
                await conn.execute(update_accounts_state_q)
                await conn.execute(update_uncles_q)
                await conn.execute(add_reorg_q)
                logger.debug('Reorg applyed for block %s %s', reorg['block_number'], reorg['block_hash'])
                return True

    async def get_last_reorg(self):
        q = """SELECT id FROM reorgs ORDER BY id DESC LIMIT 1"""
        async with self.engine.acquire() as conn:
            res = await conn.execute(q)
            row = await res.fetchone()
            last_reorg_num = row['id'] if row else 0
            return last_reorg_num

    async def update_log(self, record):
        query = logs_t.update(). \
            where(and_(logs_t.c.transaction_hash == record['transaction_hash'],
                       logs_t.c.block_hash == record['block_hash'],
                       logs_t.c.log_index == record['log_index'])). \
            values(**record)
        async with self.engine.acquire() as conn:
            return await conn.execute(query)

    async def get_logs_to_process_operations(self, limit: int) -> List[Log]:
        query = f"""
            SELECT
               is_token_transfer,
               is_transfer_processed,
               block_number 
            FROM
               logs 
            WHERE
               is_token_transfer = true 
               and is_transfer_processed = false 
            ORDER BY
               is_token_transfer,
               is_transfer_processed,
               block_number LIMIT {limit};
        """
        async with self.engine.acquire() as conn:
            unprocessed_blocks = {row['block_number'] for row in await conn.execute(query)}
            unprocessed_blocks = ', '.join(map(str, unprocessed_blocks))

        if not unprocessed_blocks:
            return []

        query = f"""
            SELECT
                address,
                block_hash,
                block_number,
                data,
                event_args,
                event_type,
                is_forked,
                is_processed,
                is_token_transfer,
                is_transfer_processed,
                log_index,
                removed,
                token_amount,
                token_transfer_from,
                token_transfer_to,
                topics,
                transaction_hash,
                transaction_index 
            FROM
               logs 
            WHERE
               is_token_transfer = true 
               AND is_transfer_processed = false 
               AND block_number in ({unprocessed_blocks})
            ORDER BY
               is_token_transfer,
               is_transfer_processed,
               block_number LIMIT {limit};
        """
        async with self.engine.acquire() as conn:
            return [dict(row) for row in await conn.execute(query)]

    async def update_token_holder_balance(self, token_address, account_address, balance):
        """
        INSERT INTO token_holders (account_address, token_address, balance) VALUES
        (%(account_address)s, %(token_address)s, %(balance)s)
        ON CONFLICT (token_address, account_address) DO UPDATE SET balance = %(param_1)s
        """
        async with self.engine.acquire() as conn:
            query = f"""
            INSERT INTO
               token_holders (token_address, account_address, balance) 
            VALUES
               (
                  '{token_address}', '{account_address}', '{balance}'
               )
               ON CONFLICT (token_address, account_address) DO 
               UPDATE
               SET
                  balance = {balance};
            """
            await conn.execute(query)


class MainDBSync(DBWrapperSync):

    def connect(self):
        engine = sync_create_engine(self.connection_string, poolclass=NullPool)
        self.conn = engine.connect()

    def is_block_exist(self, block_hash):
        q = """SELECT hash from blocks WHERE hash=%s"""
        row = self.conn.execute(q, [block_hash]).fetchone()
        return row['hash'] == block_hash if row else False

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
        if not accounts:
            return
        base_items = []
        state_items = []
        address_set = set()
        for acc in accounts:
            if acc['address'] not in address_set:
                address_set.add(acc['address'])
                base_items.append({
                    'address': acc['address'],
                    'code': acc['code'],
                    'code_hash': acc['code_hash'],
                    'last_known_balance': acc['balance'],
                    'root': acc['root'],
                })
            state_items.append({
                'block_number': acc['block_number'],
                'block_hash': acc['block_hash'],
                'address': acc['address'],
                'nonce': acc['nonce'],
                'root': acc['root'],
                'balance': acc['balance'],
            })

        base_insert = insert(accounts_base_t)
        self.conn.execute(base_insert, *base_items)
        self.conn.execute(accounts_state_t.insert(), *state_items)

    def insert_internal_transactions(self, internal_transactions):
        if internal_transactions:
            self.conn.execute(internal_transactions_t.insert(), *internal_transactions)
