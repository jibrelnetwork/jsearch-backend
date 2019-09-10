import json
import logging

import aiopg
from aiopg.sa import SAConnection
from sqlalchemy import and_, Table
from sqlalchemy.orm import Query
from typing import List, Dict, Any, Optional

from jsearch.common.processing.accounts import accounts_to_state_and_base_data
from jsearch.common.tables import (
    accounts_state_t,
    assets_summary_t,
    assets_transfers_t,
    blocks_t,
    chain_events_t,
    internal_transactions_t,
    logs_t,
    pending_transactions_t,
    receipts_t,
    token_holders_t,
    token_transfers_t,
    transactions_t,
    uncles_t,
    wallet_events_t,
)
from jsearch.syncer.database_queries.pending_transactions import insert_or_update_pending_tx_q
from jsearch.syncer.database_queries.reorgs import insert_reorg
from jsearch.typing import Blocks, Block
from .wrapper import DBWrapper

MAIN_DB_POOL_SIZE = 1

logger = logging.getLogger(__name__)


def get_update_blocks_fork_status_query(is_forked: bool, block_hashes: List[str]) -> Query:
    return blocks_t.update() \
        .values(is_forked=is_forked) \
        .where(blocks_t.c.hash.in_(block_hashes))


def get_update_fork_status_query(is_forked: bool, table: Table, block_hashes: List[str]) -> Query:
    return table.update() \
        .values(is_forked=is_forked) \
        .where(table.c.block_hash.in_(block_hashes))


class MainDB(DBWrapper):
    """
    jSearch Main db wrapper
    """
    pool_size = MAIN_DB_POOL_SIZE
    lock_conn = None

    async def disconnect(self):
        if self.lock_conn:
            self.lock_conn.close()
        self.engine.close()
        await self.engine.wait_closed()

    async def get_latest_synced_block_number(self) -> int:
        """
        Get latest block writed in main DB
        """
        q = """
            SELECT max(number) as max_number
            FROM blocks
            WHERE is_forked=false
        """
        async with self.engine.acquire() as conn:
            res = await conn.execute(q)
            row = await res.fetchone()
        return row and row['max_number'] or 0

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

    async def get_hash_map_from_block_range(self, from_block: int, to_block: int) -> Dict[str, Block]:
        query = blocks_t.select().where(and_(blocks_t.c.number > from_block, blocks_t.c.number <= to_block))
        blocks = await self.fetch_all(query)

        return {b['hash']: dict(b) for b in blocks}

    async def apply_chain_split(
            self,
            old_chain_fragment: Blocks,
            new_chain_fragment: Blocks,
            chain_event: Dict[str, Any],
    ) -> None:
        affected_chain = [*old_chain_fragment, *new_chain_fragment]

        async with self.engine.acquire() as conn:
            async with conn.begin():
                await self.update_fork_status([b['hash'] for b in old_chain_fragment], is_forked=True, conn=conn)
                await self.update_fork_status([b['hash'] for b in new_chain_fragment], is_forked=False, conn=conn)

                # write chain event
                q = chain_events_t.insert().values(**chain_event)
                await conn.execute(q)

                for block in affected_chain:
                    block_hash = block['hash']
                    reinserted = block_hash in new_chain_fragment
                    query = insert_reorg(
                        block_hash=block['hash'],
                        block_number=block['number'],
                        node_id=chain_event['node_id'],
                        split_id=chain_event['id'],
                        reinserted=reinserted
                    )
                    await conn.execute(query)

    async def update_fork_status(self, block_hashes: List[str], is_forked: bool, conn: SAConnection) -> None:
        affected_tables = (
            accounts_state_t,
            assets_summary_t,
            assets_transfers_t,
            internal_transactions_t,
            logs_t,
            receipts_t,
            token_holders_t,
            token_transfers_t,
            transactions_t,
            uncles_t,
            wallet_events_t,
        )

        await conn.execute(get_update_blocks_fork_status_query(is_forked, block_hashes))
        for table in affected_tables:
            query = get_update_fork_status_query(is_forked, table, block_hashes)
            result = await conn.execute(query)
            logger.debug(
                'Update fork status',
                extra={
                    'table': table,
                    'blocks': block_hashes,
                    'is_forked': is_forked,
                    'updated': result.rowcount
                },
            )

    async def is_block_number_exists(self, block_num):
        q = blocks_t.select().where(blocks_t.c.number == block_num)
        async with self.engine.acquire() as conn:
            res = await conn.execute(q)
            row = await res.fetchone()
            return row is not None

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

    async def write_block_data_proc(
            self,
            block_data,
            uncles_data,
            transactions_data,
            receipts_data,
            logs_data,
            accounts_data,
            internal_txs_data,
            transfers,
            token_holders_updates,
            wallet_events,
            assets_summary_updates,
            chain_event
    ):
        """
        Insert block and all related items in main database
        """
        accounts_state_data, accounts_base_data = accounts_to_state_and_base_data(accounts_data)

        token_holders_updates.sort(key=lambda u: (u['account_address'], u['token_address']))
        assets_summary_updates.sort(key=lambda u: (u['address'], u['asset_address']))

        chain_event = dict(chain_event)
        chain_event['created_at'] = chain_event['created_at'].isoformat()

        q = "SELECT FROM insert_block_data(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"

        params = [
            [block_data],
            uncles_data,
            transactions_data,
            receipts_data,
            logs_data,
            accounts_state_data,
            accounts_base_data,
            internal_txs_data,
            transfers,
            token_holders_updates,
            wallet_events,
            assets_summary_updates,
            chain_event
        ]

        async with self.engine.acquire() as connection:
            async with connection.begin():
                await connection.execute(q, *[json.dumps(item) for item in params])

    async def try_advisory_lock(self, start, end):
        if end is None:
            q = """SELECT pg_try_advisory_lock(%s)"""
            params = [start]
        else:
            q = """SELECT pg_try_advisory_lock(%s, %s)"""
            params = [start, end]

        self.lock_conn = await aiopg.connect(self.connection_string)
        cur = await self.lock_conn.cursor()
        await cur.execute(q, params)
        res = await cur.fetchone()
        return res[0]