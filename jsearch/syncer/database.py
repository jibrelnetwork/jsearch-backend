import json
import logging
from copy import copy

import aiopg
import backoff
import psycopg2
from aiopg.sa import create_engine as async_create_engine, Engine
from psycopg2.extras import DictCursor
from sqlalchemy import and_
from sqlalchemy.dialects.postgresql import insert
from typing import List, Dict, Any, Optional

from jsearch.common import contracts
from jsearch.common.processing.accounts import accounts_to_state_and_base_data
from jsearch.common.processing.wallet import ETHER_ASSET_ADDRESS, assets_from_accounts
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
    chain_events_t,
    wallet_events_t,
)
from jsearch.common.utils import as_dicts
from jsearch.syncer.balances import (
    get_token_balance_updates,
    get_last_ether_states_for_addresses_in_blocks,
    get_token_holders
)
from jsearch.syncer.database_queries.accounts import get_accounts_state_for_blocks_query
from jsearch.syncer.database_queries.assets_summary import delete_assets_summary_query, upsert_assets_summary_query
from jsearch.syncer.database_queries.pending_transactions import insert_or_update_pending_tx_q
from jsearch.typing import Blocks, Block

MAIN_DB_POOL_SIZE = 2
GENESIS_BLOCK_NUMBER = 0

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
        FROM "receipts" WHERE "block_number" BETWEEN %s AND %s
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
        q = """SELECT block_number FROM "bodies" order by block_number desc limit 1"""

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(q)
                row = await cur.fetchone()
                cur.close()

        if row:
            return row['block_number']

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

    async def get_pending_txs(self, start_id, end_id):
        q = """
        SELECT
          "id",
          "tx_hash",
          "status",
          "fields",
          "timestamp",
          "removed",
          "node_id"
        FROM "pending_transactions" WHERE "id" BETWEEN %s AND %s ORDER BY "id"
        """

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(q, [start_id, end_id])
                rows = await cur.fetchall()
                cur.close()

        logger.info(
            "Fetched batch of pending TXs",
            extra={
                'start_id': start_id,
                'end_id': end_id,
                'count': len(rows),
            },
        )

        return [dict(row) for row in rows]

    async def get_last_pending_tx_id(self) -> int:
        return await self._get_boundary_pending_tx_id(boundary='max')

    async def get_first_pending_tx_id(self) -> int:
        return await self._get_boundary_pending_tx_id(boundary='min')

    async def _get_boundary_pending_tx_id(self, boundary: str) -> int:
        if boundary not in {'min', 'max'}:
            raise ValueError(f'"boundary" must be either "min" or "max", got "{boundary}"')

        q = f'SELECT {boundary}("id") AS boundary_id FROM "pending_transactions"'

        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(q)
                row = await cur.fetchone()

        return row and row['boundary_id'] or 0

    async def get_parent_hash(self, block_hash):
        q = """SELECT fields FROM headers WHERE block_hash=%s"""
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(q, [block_hash])
                row = await cur.fetchone()
                cur.close()
        return row['fields']['parentHash']

    async def get_chain_event(self, event_id):
        q = """SELECT * from chain_events WHERE id=%s"""
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(q, [event_id])
                row = await cur.fetchone()
                cur.close()
        return row

    async def get_next_chain_event(self, block_range, event_id, node_id):
        params = [event_id, node_id]
        if block_range[1] is not None:
            block_cond = """block_number BETWEEN %s AND %s"""
            params += list(block_range)
        else:
            block_cond = """block_number >= %s"""
            params.append(block_range[0])
        q = f"""SELECT * FROM chain_events WHERE
                    id > %s
                    AND node_id=%s
                    AND {block_cond}
                  ORDER BY id ASC LIMIT 1"""
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(q, params)
                row = await cur.fetchone()
                cur.close()
        return row

    async def get_first_chain_event_for_block_range(self, block_range, node_id):
        if block_range[1] is not None:
            cond = """block_number BETWEEN %s AND %s"""
            params = list(block_range)
        else:
            cond = """block_number >= %s"""
            params = [block_range[0]]
        params.append(node_id)

        q = f"""
            SELECT * FROM chain_events
            WHERE {cond}  AND node_id=%s
            ORDER BY id ASC LIMIT 1
        """
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(q, params)
                row = await cur.fetchone()
                cur.close()
        return row

    async def get_chain_split(self, split_id):
        q = """SELECT * from chain_splits WHERE id=%s"""
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(q, [split_id])
                row = await cur.fetchone()
                cur.close()
        return row

    async def is_canonical_block(self, block_hash):
        q = """SELECT id, reinserted FROM reorgs WHERE block_hash=%s ORDER BY id DESC"""
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(q, [block_hash])
                rows = await cur.fetchall()
                cur.close()
        if len(rows) == 0:
            return True
        if rows[0]['reinserted'] is True:
            return True
        return False

    async def fetch_rows(self, q, params):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(q, params)
                rows = await cur.fetchall()
                cur.close()
        return rows

    async def fetch_row(self, q, params):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(q, params)
                row = await cur.fetchone()
                cur.close()
        return row

    async def get_header_by_hash(self, block_hash):
        q = """SELECT "block_number", "block_hash", "fields" FROM "headers" WHERE "block_hash"=%s"""
        row = await self.fetch_row(q, [block_hash])
        return row

    async def get_header_by_block_number(self, block_number):
        q = """SELECT "block_number", "block_hash", "fields" FROM "headers" WHERE "block_number"=%s"""
        row = await self.fetch_row(q, [block_number])
        return row

    async def get_block_accounts(self, block_hash):
        q = """SELECT "id", "block_number", "block_hash", "address", "fields" FROM "accounts" WHERE "block_hash"=%s"""
        rows = await self.fetch_rows(q, [block_hash])
        return rows

    async def get_block_body(self, block_hash):
        q = """SELECT "block_number", "block_hash", "fields" FROM "bodies" WHERE "block_hash"=%s"""
        row = await self.fetch_row(q, [block_hash])
        return row

    async def get_block_receipts(self, block_hash):
        q = """SELECT "block_number", "block_hash", "fields" FROM "receipts" WHERE "block_hash"=%s"""
        row = await self.fetch_row(q, [block_hash])
        return row

    async def get_reward(self, block_number, block_hash):
        if block_number == GENESIS_BLOCK_NUMBER:
            return get_reward_for_genesis_block(block_hash)

        q = """SELECT "id", "block_number", "block_hash", "address", "fields" FROM "rewards" WHERE "block_hash"=%s"""
        rows = await self.fetch_rows(q, [block_hash])
        if len(rows) > 1:
            for r in rows:
                if r['address'] != contracts.NULL_ADDRESS:
                    return r
        elif len(rows) == 1:
            return rows[0]
        else:
            return None

    async def get_internal_transactions(self, block_hash):

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

        rows = await self.fetch_rows(q, [block_hash])
        return rows


class RawDBSync(DBWrapper):
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

    def get_reward(self, block_number, block_hash):
        if block_number == GENESIS_BLOCK_NUMBER:
            return get_reward_for_genesis_block(block_hash)

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

    @backoff.on_exception(backoff.fibo, max_tries=10, exception=psycopg2.OperationalError)
    async def fetch_one(self, query, *params):
        async with self.engine.acquire() as connection:
            cursor = await connection.execute(query, params)
            return await cursor.fetchone()

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

    async def get_blockchain_heads(self, blocks_range):
        """
        Get blockchain head (or heads) - blocks with maximum number
        """
        if blocks_range[1] is None:
            condition = 'number >= %s'
            params = (blocks_range[0],)
        else:
            condition = 'number BETWEEN %s AND %s'
            params = blocks_range

        q = """
            SELECT * FROM blocks WHERE number = (SELECT MAX(number)
                FROM blocks
                WHERE is_forked=false AND {cond}""".format(cond=condition)
        async with self.engine.acquire() as conn:
            res = await conn.execute(q, params)
            rows = await res.fetchall()
            return rows

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

    async def get_accounts_addresses_for_blocks(self, blocks_hashes: List[str]) -> List[str]:
        query = get_accounts_state_for_blocks_query(blocks_hashes=blocks_hashes)
        return list({item['address'] for item in await self.fetch_all(query)})

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

    async def get_hash_map_from_block_range(self, from_block: int, to_block: int) -> Dict[str, Block]:
        query = blocks_t.select().where(and_(blocks_t.c.number > from_block, blocks_t.c.number <= to_block))
        blocks = await self.fetch_all(query)

        return {b['hash']: dict(b) for b in blocks}

    async def apply_chain_split(
            self,
            old_chain_fragment: Blocks,
            new_chain_fragment: Blocks,
            chain_event: Dict[str, Any],
            last_block: int
    ) -> None:

        async with self.engine.acquire() as conn:
            async with conn.begin():
                await self.update_fork_status([b['hash'] for b in old_chain_fragment], is_forked=True, conn=conn)
                await self.update_fork_status([b['hash'] for b in new_chain_fragment], is_forked=False, conn=conn)
                affected_blocks = list(
                    {b['hash'] for b in old_chain_fragment} | {b['hash'] for b in new_chain_fragment}
                )

                token_holders = await get_token_holders(conn, blocks_hashes=affected_blocks)
                token_updates = await get_token_balance_updates(
                    connection=conn,
                    token_holders=token_holders,
                    block=last_block
                )

                # get ether balance updates
                accounts_addresses = await self.get_accounts_addresses_for_blocks(affected_blocks)
                accounts_states = await get_last_ether_states_for_addresses_in_blocks(conn, affected_blocks)
                accounts_states_map = {item['address']: item for item in accounts_states}

                delete_states = set(accounts_addresses) - set(accounts_states_map.keys())
                ether_updates = assets_from_accounts(accounts=accounts_states)

                # affected_address
                for balance_update in token_updates:
                    query = balance_update.to_upsert_assets_summary_query()
                    await conn.execute(query)

                    query = balance_update.to_upsert_token_holder_query()
                    await conn.execute(query)

                for account_state in ether_updates:
                    query = upsert_assets_summary_query(**account_state)
                    await conn.execute(query)

                for address in delete_states:
                    query = delete_assets_summary_query(address=address, asset_address=ETHER_ASSET_ADDRESS)
                    await conn.execute(query)

                # write chain event
                q = chain_events_t.insert().values(**chain_event)
                await conn.execute(q)

    async def update_fork_status(self, block_hashes, is_forked, conn):
        update_block_q = blocks_t.update() \
            .values(is_forked=is_forked) \
            .where(blocks_t.c.hash.in_(block_hashes)) \
            .returning(blocks_t.c.hash)

        update_txs_q = transactions_t.update() \
            .values(is_forked=is_forked) \
            .where(transactions_t.c.block_hash.in_(block_hashes))

        update_receipts_q = receipts_t.update() \
            .values(is_forked=is_forked) \
            .where(receipts_t.c.block_hash.in_(block_hashes))

        update_logs_q = logs_t.update() \
            .values(is_forked=is_forked) \
            .where(logs_t.c.block_hash.in_(block_hashes))

        update_token_transfers_q = token_transfers_t.update() \
            .values(is_forked=is_forked) \
            .where(token_transfers_t.c.block_hash.in_(block_hashes))

        update_assets_transfers_q = assets_transfers_t.update() \
            .values(is_forked=is_forked) \
            .where(assets_transfers_t.c.block_hash.in_(block_hashes))

        update_internal_transactions_q = internal_transactions_t.update() \
            .values(is_forked=is_forked) \
            .where(internal_transactions_t.c.block_hash.in_(block_hashes))

        update_accounts_state_q = accounts_state_t.update() \
            .values(is_forked=is_forked) \
            .where(accounts_state_t.c.block_hash.in_(block_hashes))

        update_uncles_q = uncles_t.update() \
            .values(is_forked=is_forked) \
            .where(uncles_t.c.block_hash.in_(block_hashes))

        update_wallet_events_q = wallet_events_t.update() \
            .values(is_forked=is_forked) \
            .where(wallet_events_t.c.block_hash.in_(block_hashes))

        await conn.execute(update_block_q)
        await conn.execute(update_txs_q)
        await conn.execute(update_receipts_q)
        await conn.execute(update_logs_q)
        await conn.execute(update_internal_transactions_q)
        await conn.execute(update_accounts_state_q)
        await conn.execute(update_uncles_q)
        await conn.execute(update_token_transfers_q)
        await conn.execute(update_assets_transfers_q)
        await conn.execute(update_wallet_events_q)
        logger.debug(
            'Update fork status',
            extra={
                'blocks': block_hashes,
                'is_forked': is_forked,
            },
        )

    async def is_block_number_exists(self, block_num):
        q = blocks_t.select().where(blocks_t.c.number == block_num)
        async with self.engine.acquire() as conn:
            res = await conn.execute(q)
            row = await res.fetchone()
            return row is not None

    async def is_canonical_block(self, block_hash):
        q = blocks_t.select().where(blocks_t.c.hash == block_hash)
        async with self.engine.acquire() as conn:
            res = await conn.execute(q)
            row = await res.fetchone()
            return not row['is_forked']

    async def get_last_chain_split(self) -> int:
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

    async def get_last_chain_event(self, sync_range, node_id):
        if sync_range[1] is not None:
            cond = """block_number BETWEEN %s AND %s"""
            params = list(sync_range)
        else:
            cond = """block_number >= %s"""
            params = [sync_range[0]]

        params.insert(0, node_id)
        q = f"""SELECT * FROM chain_events
                    WHERE node_id=%s AND ({cond})
                    ORDER BY id DESC LIMIT 1"""
        async with self.engine.acquire() as conn:
            res = await conn.execute(q, params)
            row = await res.fetchone()
            return dict(row) if row else None

    async def insert_chain_event(self, event):
        q = chain_events_t.insert().values(**event)
        async with self.engine.acquire() as conn:
            await conn.execute(q)

    async def get_pending_tx_last_synced_id(self) -> Optional[int]:
        q = pending_transactions_t.select()
        q = q.order_by(pending_transactions_t.c.last_synced_id.desc())
        q = q.limit(1)

        async with self.engine.acquire() as conn:
            res = await conn.execute(q)
            row = await res.fetchone()

        return row['last_synced_id'] if row else None

    async def insert_or_update_pending_tx(self, pending_tx: Dict[str, Any]) -> None:
        query = insert_or_update_pending_tx_q(pending_tx)
        await self.execute(query)

    async def is_block_exist(self, block_hash):
        q = """SELECT hash from blocks WHERE hash=%s"""
        row = await self.fetch_one(q, block_hash)
        return row['hash'] == block_hash if row else False

    async def write_block_data_proc(self, block_data, uncles_data, transactions_data, receipts_data,
                                    logs_data, accounts_data, internal_txs_data, transfers,
                                    token_holders_updates, wallet_events, assets_summary_updates, chain_event):
        """
        Insert block and all related items in main database
        """
        accounts_state_data, accounts_base_data = accounts_to_state_and_base_data(accounts_data)

        token_holders_updates.sort(key=lambda u: (u['account_address'], u['token_address']))
        assets_summary_updates.sort(key=lambda u: (u['address'], u['asset_address']))

        chain_event = dict(chain_event)
        chain_event['created_at'] = chain_event['created_at'].isoformat()

        q = "SELECT FROM insert_block_data(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
        j = json.dumps

        async with self.engine.acquire() as conn:
            async with conn.begin():
                await self.execute(q, [j([block_data]), j(uncles_data), j(transactions_data),
                                       j(receipts_data), j(logs_data), j(accounts_state_data),
                                       j(accounts_base_data), j(internal_txs_data), j(transfers),
                                       j(token_holders_updates), j(wallet_events), j(assets_summary_updates),
                                       j(chain_event)])

    async def insert_block(self, block_data):
        if block_data:
            await self.execute(blocks_t.insert(), block_data)

    async def insert_uncles(self, uncles_data):
        if uncles_data:
            await self.execute(uncles_t.insert(), *uncles_data)

    async def insert_transactions(self, transactions_data):
        if transactions_data:
            transactions = []
            for td in transactions_data:
                tx1 = copy(td)
                tx1['address'] = tx1['from']
                tx2 = copy(td)
                tx2['address'] = tx2['to']
                transactions.append(tx1)
                transactions.append(tx2)
            await self.execute(transactions_t.insert(), *transactions)

    async def insert_receipts(self, receipts_data):
        if receipts_data:
            await self.execute(receipts_t.insert(), *receipts_data)

    async def insert_logs(self, logs_data):
        if logs_data:
            await self.execute(logs_t.insert(), *logs_data)

    async def insert_accounts(self, accounts):
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
        await self.execute(base_insert, *base_items)
        await self.execute(accounts_state_t.insert(), *state_items)

    async def insert_internal_transactions(self, internal_transactions):
        if internal_transactions:
            self.execute(internal_transactions_t.insert(), *internal_transactions)

    async def insert_or_update_transfers(self, records: List[Dict[str, Any]]):
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
            await self.execute(insert_query)

    def update_log(self, tx_hash, block_hash, log_index, values: Dict[str, Any]):
        query = logs_t.update(). \
            where(and_(logs_t.c.transaction_hash == tx_hash,
                       logs_t.c.block_hash == block_hash,
                       logs_t.c.log_index == log_index)). \
            values(**values)
        self.execute(query)

    @as_dicts
    async def get_blocks(self, hashes):
        query = blocks_t.select().where(blocks_t.c.hash.in_(hashes))
        return await self.execute(query)


def get_reward_for_genesis_block(block_hash):
    # WTF: There's no reward for the genesis block, so this func makes a dummy
    # row for the `Syncer` to process.
    return {
        "id": 0,
        "block_number": 0,
        "block_hash": block_hash,
        "address": None,
        "fields": {
            "Uncles": [],
            "TimeStamp": 0,
            "TxsReward": 0,
            "BlockMiner": None,
            "BlockNumber": 0,
            "BlockReward": 0,
            "UnclesReward": 0,
            "UncleInclusionReward": 0
        },
    }
