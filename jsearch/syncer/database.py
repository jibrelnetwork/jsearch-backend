import logging
from copy import copy
from typing import List, Dict, Any

import aiopg
import backoff
import psycopg2
from aiopg.sa import create_engine as async_create_engine, Engine
from psycopg2.extras import DictCursor
from sqlalchemy import create_engine as sync_create_engine, and_
from sqlalchemy.dialects.postgresql import insert
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
    token_transfers_t,
    chain_splits_t,
    pending_transactions_t,
    assets_transfers_t,
    wallet_events_t)
from jsearch.common.utils import as_dicts
from jsearch.syncer.database_queries.pending_transactions import insert_or_update_pending_tx_q
from jsearch.syncer.database_queries.token_holders import update_token_holder_balance_q
from jsearch.syncer.database_queries.token_transfers import (
    get_transfers_from_query,
    get_transfers_to_query
)

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


class RawDB(DBWrapper):
    """
    jSearch RAW db wrapper
    """

    async def get_blocks_to_sync(self, start_block_num=0, end_block_num=None):
        q = """
        SELECT
          "block_hash",
          "block_number"
        FROM "headers" WHERE "block_number" BETWEEN %s AND %s
        """

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(q, [start_block_num, end_block_num])
                rows = await cur.fetchall()
                cur.close()

        return rows

    async def get_missed_blocks(self, blocks_numbers):
        if not blocks_numbers:
            return []

        q = """
        SELECT
          "block_hash",
          "block_number"
        FROM "headers" WHERE "block_number" IN ({})""".format(','.join('%s' for _ in blocks_numbers))

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(q, blocks_numbers)
                rows = await cur.fetchall()
                cur.close()

        return rows

    async def get_latest_available_block_number(self):
        q = """SELECT max("block_number") AS "max_block" FROM "bodies" """

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(q)
                row = await cur.fetchone()
                cur.close()

        return row['max_block'] or None

    async def get_reorgs_by_chain_split_id(self, chain_split_id):
        q = """
        SELECT
          "id",
          "block_number",
          "block_hash",
          "header",
          "reinserted",
          "node_id",
          "split_id"
        FROM "reorgs" WHERE "split_id" = %s
        """

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(q, [chain_split_id])
                rows = await cur.fetchall()
                cur.close()

        return rows

    async def get_chain_splits_from(self, split_from_num, limit):
        q = """
        SELECT
          "id",
          "common_block_number",
          "common_block_hash",
          "drop_length",
          "drop_block_hash",
          "add_length",
          "add_block_hash",
          "node_id"
        FROM "chain_splits" WHERE "id" > %s ORDER BY "id" LIMIT %s
        """

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(q, [split_from_num, limit])
                rows = await cur.fetchall()
                cur.close()

        return rows

    async def get_pending_txs_from(self, last_synced_id, limit):
        q = """
        SELECT
          "id",
          "tx_hash",
          "status",
          "fields",
          "timestamp",
          "removed",
          "node_id"
        FROM "pending_transactions" WHERE "id" > %s ORDER BY "id" LIMIT %s
        """

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(q, [last_synced_id, limit])
                rows = await cur.fetchall()
                cur.close()

        return [dict(row) for row in rows]


class RawDBSync(DBWrapperSync):

    def connect(self):
        self.conn = psycopg2.connect(self.connection_string, cursor_factory=DictCursor)

    def disconnect(self):
        self.conn.close()

    def get_header_by_hash(self, block_number):
        q = """SELECT "block_number", "block_hash", "fields" FROM "headers" WHERE "block_hash"=%s"""
        with self.conn.cursor() as cur:
            cur.execute(q, [block_number])
            row = cur.fetchone()
        return row

    def get_header_by_block_number(self, block_number):
        q = """SELECT "block_number", "block_hash", "fields" FROM "headers" WHERE "block_number"=%s"""
        with self.conn.cursor() as cur:
            cur.execute(q, [block_number])
            row = cur.fetchone()
        return row

    def get_block_accounts(self, block_hash):
        q = """SELECT "id", "block_number", "block_hash", "address", "fields" FROM "accounts" WHERE "block_hash"=%s"""
        with self.conn.cursor() as cur:
            cur.execute(q, [block_hash])
            rows = cur.fetchall()
        return rows

    def get_block_body(self, block_hash):
        q = """SELECT "block_number", "block_hash", "fields" FROM "bodies" WHERE "block_hash"=%s"""
        with self.conn.cursor() as cur:
            cur.execute(q, [block_hash])
            row = cur.fetchone()
        return row

    def get_block_receipts(self, block_hash):
        q = """SELECT "block_number", "block_hash", "fields" FROM "receipts" WHERE "block_hash"=%s"""
        with self.conn.cursor() as cur:
            cur.execute(q, [block_hash])
            row = cur.fetchone()
        return row

    def get_reward(self, block_hash):
        q = """SELECT "id", "block_number", "block_hash", "address", "fields" FROM "rewards" WHERE "block_hash"=%s"""
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
        q = """
        SELECT
          "id",
          "block_number",
          "block_hash",
          "parent_tx_hash",
          "index",
          "type",
          "timestamp",
          "fields"
        FROM "internal_transactions" WHERE "block_hash"=%s"""

        with self.conn.cursor() as cur:
            cur.execute(q, [block_hash])
            rows = cur.fetchall()

        return rows


class MainDB(DBWrapper):
    """
    jSearch Main db wrapper
    """

    engine: Engine

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()
        if exc_type:
            return False

    async def connect(self):
        self.engine = await async_create_engine(self.connection_string, minsize=1, maxsize=MAIN_DB_POOL_SIZE)

    async def disconnect(self):
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
                FROM (SELECT "number" FROM blocks WHERE is_forked = false) as l
                LEFT OUTER JOIN blocks as r ON l.number + 1 = r.number AND r.is_forked = false
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
            .where(transactions_t.c.block_hash == reorg['block_hash'])

        update_receipts_q = receipts_t.update() \
            .values(is_forked=not reorg['reinserted']) \
            .where(receipts_t.c.block_hash == reorg['block_hash'])

        update_logs_q = logs_t.update() \
            .values(is_forked=not reorg['reinserted']) \
            .where(logs_t.c.block_hash == reorg['block_hash'])

        update_token_transfers_q = token_transfers_t.update() \
            .values(is_forked=not reorg['reinserted']) \
            .where(token_transfers_t.c.block_hash == reorg['block_hash'])

        update_assets_transfers_q = assets_transfers_t.update() \
            .values(is_forked=not reorg['reinserted']) \
            .where(assets_transfers_t.c.block_hash == reorg['block_hash'])

        update_internal_transactions_q = internal_transactions_t.update() \
            .values(is_forked=not reorg['reinserted']) \
            .where(internal_transactions_t.c.block_hash == reorg['block_hash'])

        update_accounts_state_q = accounts_state_t.update() \
            .values(is_forked=not reorg['reinserted']) \
            .where(accounts_state_t.c.block_hash == reorg['block_hash'])

        update_events_q = wallet_events_t.update() \
            .values(is_forked=not reorg['reinserted']) \
            .where(wallet_events_t.c.block_hash == reorg['block_hash'])

        update_uncles_q = uncles_t.update() \
            .values(is_forked=not reorg['reinserted']) \
            .where(uncles_t.c.block_hash == reorg['block_hash'])

        reorg.pop('header')
        add_reorg_q = reorgs_t.insert().values(**reorg)
        async with self.engine.acquire() as conn:
            async with conn.begin() as tx:
                res = await conn.execute(update_block_q)
                rows = await res.fetchall()
                if len(rows) == 0:
                    # no updates, block is not synced - aborting reorg
                    logger.debug(
                        'Aborting reorg for a block',
                        extra={
                            'block_hash': reorg['block_hash'],
                            'block_number': reorg['block_number'],
                        },
                    )
                    await tx.rollback()
                    return False
                await conn.execute(update_txs_q)
                await conn.execute(update_receipts_q)
                await conn.execute(update_logs_q)
                await conn.execute(update_internal_transactions_q)
                await conn.execute(update_accounts_state_q)
                await conn.execute(update_uncles_q)
                await conn.execute(add_reorg_q)
                await conn.execute(update_token_transfers_q)
                await conn.execute(update_assets_transfers_q)
                await conn.execute(update_events_q)

                logger.debug(
                    'Reord is applied for a block',
                    extra={
                        'block_hash': reorg['block_hash'],
                        'block_number': reorg['block_number'],
                    },
                )
                return True

    async def get_last_chain_split(self):
        q = """SELECT id FROM chain_splits ORDER BY id DESC LIMIT 1"""
        async with self.engine.acquire() as conn:
            res = await conn.execute(q)
            row = await res.fetchone()
            last_chain_split_num = row['id'] if row else 0
            return last_chain_split_num

    async def insert_chain_split(self, split):
        q = chain_splits_t.insert().values(**split)
        async with self.engine.acquire() as conn:
            await conn.execute(q)

    @as_dicts
    async def get_blocks(self, hashes: List[str]):
        query = blocks_t.select().where(blocks_t.c.hash.in_(hashes))
        return await self.fetch_all(query)

    async def get_pending_tx_last_synced_id(self) -> int:
        q = pending_transactions_t.select()
        q = q.order_by(pending_transactions_t.c.last_synced_id.desc())
        q = q.limit(1)

        async with self.engine.acquire() as conn:
            res = await conn.execute(q)
            row = await res.fetchone()

        return row['last_synced_id'] if row else 0

    async def insert_or_update_pending_txs(self, pending_txs: List[Dict[str, Any]]) -> None:
        for pending_tx in pending_txs:
            await self.execute(insert_or_update_pending_tx_q(pending_tx))


class MainDBSync(DBWrapperSync):

    def connect(self):
        engine = sync_create_engine(self.connection_string, poolclass=NullPool)
        self.conn = engine.connect()

    @backoff.on_exception(backoff.fibo, max_tries=10, exception=psycopg2.OperationalError)
    def execute(self, query, *args, **kwargs):
        return self.conn.execute(query, *args, **kwargs)

    def fetch_one(self, query, *args, **kwargs):
        return self.execute(query, *args, **kwargs).fetchone()

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
            self.execute(blocks_t.insert(), block_data)

    def insert_uncles(self, uncles_data):
        if uncles_data:
            self.execute(uncles_t.insert(), *uncles_data)

    def insert_transactions(self, transactions_data):
        if transactions_data:
            transactions = []
            for td in transactions_data:
                tx1 = copy(td)
                tx1['address'] = tx1['from']
                tx2 = copy(td)
                tx2['address'] = tx2['to']
                transactions.append(tx1)
                transactions.append(tx2)
            self.execute(transactions_t.insert(), *transactions)

    def insert_receipts(self, receipts_data):
        if receipts_data:
            self.execute(receipts_t.insert(), *receipts_data)

    def insert_logs(self, logs_data):
        if logs_data:
            self.execute(logs_t.insert(), *logs_data)

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
        self.execute(base_insert, *base_items)
        self.execute(accounts_state_t.insert(), *state_items)

    def insert_internal_transactions(self, internal_transactions):
        if internal_transactions:
            self.execute(internal_transactions_t.insert(), *internal_transactions)

    def insert_or_update_transfers(self, records: List[Dict[str, Any]]):
        for i, record in enumerate(records):
            insert_query = insert(token_transfers_t).values(record).on_conflict_do_update(
                index_elements=[
                    'transaction_hash',
                    'log_index',
                    'address',
                ],
                set_={
                    'block_number': record['block_number'],
                    'block_hash': record['block_hash'],
                    'transaction_index': record['transaction_index'],
                    'timestamp': record['timestamp'],
                    'from_address': record['from_address'],
                    'to_address': record['to_address'],
                    'token_address': record['token_address'],
                    'token_decimals': record['token_decimals'],
                    'token_name': record['token_name'],
                    'token_symbol': record['token_symbol'],
                    'token_value': record['token_value'],
                }
            )
            self.execute(insert_query)

    def update_token_holder_balance(self, token_address: str, account_address: str, balance: int, decimals: int):
        query = update_token_holder_balance_q(
            token_address=token_address,
            account_address=account_address,
            balance=balance,
            decimals=decimals
        )
        return self.execute(query)

    @as_dicts
    def get_blocks(self, hashes):
        query = blocks_t.select().where(blocks_t.c.hash.in_(hashes))
        return self.execute(query)

    def get_balance_changes_since_block(self, token: str, account: str, block_number: int) -> int:
        positive_changes_query = get_transfers_to_query(token, account, block_number)
        positive_changes = self.fetch_one(positive_changes_query)['value']

        negative_changes_query = get_transfers_from_query(token, account, block_number)
        negative_changes = self.fetch_one(negative_changes_query)['value']

        return (positive_changes or 0) - (negative_changes or 0)
