import json
import logging
from typing import List, Dict, Any, Optional

from aiopg.sa import SAConnection
from sqlalchemy import and_, Table, false, select, desc
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Query

from jsearch.common.db import DBWrapper
from jsearch.common.processing.accounts import accounts_to_state_and_base_data
from jsearch.common.structs import BlockRange
from jsearch.common.tables import (
    accounts_state_t,
    assets_summary_t,
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
    dex_logs_t)
from jsearch.common.utils import timeit
from jsearch.pending_syncer.database_queries.pending_txs import insert_or_update_pending_txs_q
from jsearch.syncer.database_queries.reorgs import insert_reorg
from jsearch.syncer.structs import BlockData
from jsearch.typing import Blocks, Block

MAIN_DB_POOL_SIZE = 1
NO_CODE_HASH = 'c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470'

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

    async def get_block_by_hash(self, block_hash: str) -> Block:
        query = blocks_t.select().where(blocks_t.c.hash == block_hash)
        return await self.fetch_one(query)

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
                q = insert(chain_events_t).values(**chain_event).on_conflict_do_nothing(index_elements=['id'])
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
            internal_transactions_t,
            logs_t,
            receipts_t,
            token_holders_t,
            token_transfers_t,
            transactions_t,
            uncles_t,
            wallet_events_t,
            dex_logs_t
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

    @timeit('[MAIN DB] is block exists query')
    async def is_block_number_exists(self, block_num):
        q = blocks_t.select().where(
            and_(
                blocks_t.c.number == block_num,
                blocks_t.c.is_forked == false()
            )
        )
        async with self.engine.acquire() as conn:
            res = await conn.execute(q)
            row = await res.fetchone()
            return row is not None

    async def get_set_borders(self, start: int, end: int) -> Optional[BlockRange]:
        query = """
            select min(number) as left, max(number) as right
            from blocks
            where is_forked = false and number >= %s and number <= %s;
        """
        result = await self.fetch_one(query, start, end)
        if result and (result['left'] or result['right']):
            return BlockRange(result['left'], result['right'])

        return None

    async def get_gap_right_border(self, start: int, end: int) -> Optional[int]:
        query = """
        select next_number - 1 as gap_end
        from (
          select number, lead(number) over (order by number asc) as next_number
          from blocks
          where is_forked = false and blocks.number >= %s and blocks.number <= %s
        ) numbers
        where number + 1 <> next_number limit 1;
        """
        result = await self.fetch_one(query, start, end + 1)
        if result:
            return result['gap_end']

        return None

    @timeit(name='Query to find gaps')
    async def check_on_holes(self, start: int, end: int) -> Optional[BlockRange]:
        gap_end = None
        # check blocks to prevent case:
        # | | | |x|x|x|
        blocks = await self.get_set_borders(start, end)

        if blocks and blocks.start > start:
            gap_end = blocks.start - 1
            gap_end = gap_end if gap_end > start else start

        if gap_end is None:
            gap_end = await self.get_gap_right_border(start, end)

        if gap_end is not None:

            # we have found right border of gap
            # let's find left border
            # |x|x|x| | | | |x|x|
            blocks = await self.get_set_borders(start, gap_end)

            # FIXME (nickgashkov): `BlockRange.end` could be `None`.
            if blocks and blocks.end > start:  # type: ignore
                gap_start = blocks.end + 1  # type: ignore
            else:
                gap_start = start

            gap = BlockRange(gap_start, gap_end)
            logger.info(
                'Gap was founded',
                extra={
                    'gap': gap,
                    'range': BlockRange(start, end)
                }
            )
            return gap

        return None

    @timeit('[MAIN DB] Get last block')
    async def get_last_block_number(self, block_range: BlockRange) -> int:
        query = select([blocks_t.c.number]).order_by(desc(blocks_t.c.number)).limit(1)

        if block_range.start:
            query = query.where(blocks_t.c.number >= block_range.start)
        if block_range.end:
            query = query.where(blocks_t.c.number <= block_range.end)

        result = await self.fetch_one(query)
        if result:
            return result['number']
        return 0

    @timeit('[MAIN DB] Get last chain event')
    async def get_last_chain_event(self, sync_range: BlockRange, node_id: str) -> Optional[Dict[str, Any]]:
        if sync_range.end is not None:
            cond = """block_number >= %s AND block_number <= %s"""
            params = list(sync_range)
        else:
            cond = """block_number >= %s"""
            params = [sync_range.start]

        params.insert(0, node_id)  # type: ignore
        q = f"""
            SELECT * FROM chain_events
            WHERE node_id=%s AND ({cond})
            ORDER BY id DESC LIMIT 1
        """
        async with self.engine.acquire() as conn:
            res = await conn.execute(q, params)
            row = await res.fetchone()
            return dict(row) if row else None

    @timeit('Insert chain event')
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

    async def insert_or_update_pending_txs(self, pending_txs: List[Dict[str, Any]]) -> None:
        query = insert_or_update_pending_txs_q(pending_txs)

        async with self.engine.acquire() as conn:
            res = await conn.execute(query)

        logger.info('Upserted batch of pending TXs', extra={'status_message': res._connection._cursor.statusmessage})

    async def write_block_data_proc(
            self,
            chain_event: Dict[str, Any],
            block_data: BlockData,
            connection: SAConnection
    ) -> None:
        """
        Insert block and all related items in main database
        """
        accounts_state_data, accounts_base_data = accounts_to_state_and_base_data(block_data.accounts)

        block_data.token_holders_updates.sort(key=lambda u: (u['account_address'], u['token_address']))
        block_data.assets_summary_updates.sort(key=lambda u: (u['address'], u['asset_address']))

        if chain_event is not None:
            chain_event = dict(chain_event)
            chain_event['created_at'] = chain_event['created_at'].isoformat()
        else:
            chain_event = ''

        q = "SELECT FROM insert_block_data(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"

        params = [
            [block_data.block],
            block_data.uncles,
            block_data.txs,
            block_data.receipts,
            block_data.logs,
            accounts_state_data,
            accounts_base_data,
            block_data.internal_txs,
            block_data.transfers,
            block_data.token_holders_updates,
            block_data.wallet_events,
            block_data.assets_summary_updates,
            block_data.assets_summary_pairs,
            chain_event,
            block_data.dex_events
        ]
        await connection.execute(q, *[json.dumps(item) for item in params])

    @timeit('[MAIN DB] Write block')
    async def write_block(self, chain_event: Optional[Dict[str, Any]], block_data: BlockData, rewrite: bool):
        async with self.engine.acquire() as connection:
            async with connection.begin():
                if rewrite:
                    await self.delete_block_data(block_data.block['hash'], connection)
                await self.write_block_data_proc(chain_event, block_data, connection)

    async def delete_block_data(self, block_hash, connection):
        logger.info('deleting block %s', block_hash)
        await connection.execute("""DELETE FROM token_holders WHERE block_hash=%s""", block_hash)
        await connection.execute("""DELETE FROM wallet_events WHERE block_hash=%s""", block_hash)
        await connection.execute("""DELETE FROM assets_summary WHERE block_hash=%s""", block_hash)
        await connection.execute("""DELETE FROM token_transfers WHERE block_hash=%s""", block_hash)
        await connection.execute("""DELETE FROM transactions WHERE block_hash=%s""", block_hash)
        await connection.execute("""DELETE FROM logs WHERE block_hash=%s""", block_hash)
        await connection.execute("""DELETE FROM receipts WHERE block_hash=%s""", block_hash)
        await connection.execute("""DELETE FROM accounts_state WHERE block_hash=%s""", block_hash)
        await connection.execute("""DELETE FROM internal_transactions WHERE block_hash=%s""", block_hash)
        await connection.execute("""DELETE FROM uncles WHERE block_hash=%s""", block_hash)
        await connection.execute("""DELETE FROM blocks WHERE hash=%s""", block_hash)
        await connection.execute("""DELETE FROM dex_logs WHERE hash=%s""", block_hash)

    async def get_block_hash_by_number(self, block_num) -> Optional[str]:
        q = blocks_t.select().where(and_(blocks_t.c.number == block_num, blocks_t.c.is_forked == false()))
        async with self.engine.acquire() as conn:
            res = await conn.execute(q)
            row = await res.fetchone()
            if row:
                return row['hash']

        return None

    async def is_contract_address(self, addresses):
        contracts_addresses = []
        q = """SELECT code_hash FROM accounts_base WHERE address = %s limit 1"""
        for address in addresses:
            row = await self.fetch_one(q, address)
            if row is not None and row['code_hash'] != NO_CODE_HASH:
                contracts_addresses.append(address)
        return contracts_addresses
