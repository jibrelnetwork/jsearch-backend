import json
import logging

from sqlalchemy import and_
from typing import List, Dict, Any, Optional

from jsearch.common.processing.accounts import accounts_to_state_and_base_data
from jsearch.common.processing.wallet import ETHER_ASSET_ADDRESS, assets_from_accounts
from jsearch.common.tables import (
    accounts_state_t,
    blocks_t,
    internal_transactions_t,
    logs_t,
    receipts_t,
    transactions_t,
    uncles_t,
    token_transfers_t,
    pending_transactions_t,
    assets_transfers_t,
    chain_events_t,
    wallet_events_t,
)
from jsearch.syncer.database_queries.accounts import get_accounts_state_for_blocks_query
from jsearch.syncer.database_queries.assets_summary import delete_assets_summary_query, upsert_assets_summary_query
from jsearch.syncer.database_queries.pending_transactions import insert_or_update_pending_tx_q
from jsearch.syncer.database_queries.reorgs import insert_reorg
from jsearch.syncer.utils.balances import (
    get_last_ether_states_for_addresses_in_blocks,
    get_token_holders,
    filter_negative_balances,
    get_token_balance_updates
)
from jsearch.typing import Blocks, Block
from .wrapper import DBWrapper

MAIN_DB_POOL_SIZE = 2

logger = logging.getLogger(__name__)


class MainDB(DBWrapper):
    """
    jSearch Main db wrapper
    """
    pool_size = MAIN_DB_POOL_SIZE

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

    async def get_accounts_addresses_for_blocks(self, blocks_hashes: List[str]) -> List[str]:
        query = get_accounts_state_for_blocks_query(blocks_hashes=blocks_hashes)
        return list({item['address'] for item in await self.fetch_all(query)})

    async def get_hash_map_from_block_range(self, from_block: int, to_block: int) -> Dict[str, Block]:
        query = blocks_t.select().where(and_(blocks_t.c.number > from_block, blocks_t.c.number <= to_block))
        blocks = await self.fetch_all(query)

        return {b['hash']: dict(b) for b in blocks}

    async def apply_chain_split(
            self,
            old_chain_fragment: Blocks,
            new_chain_fragment: Blocks,
            chain_event: Dict[str, Any],
            last_block: int,
    ) -> None:
        affected_chain = [*old_chain_fragment, *new_chain_fragment]

        old_block_hashes = [block['hash'] for block in old_chain_fragment]
        new_block_hashes = [block['hash'] for block in new_chain_fragment]
        affected_blocks = list(set(old_block_hashes) | set(new_block_hashes))

        async with self.engine.acquire() as conn:
            async with conn.begin():
                await self.update_fork_status([b['hash'] for b in old_chain_fragment], is_forked=True, conn=conn)
                await self.update_fork_status([b['hash'] for b in new_chain_fragment], is_forked=False, conn=conn)

                token_holders = await get_token_holders(conn, blocks_hashes=affected_blocks)
                token_updates = await get_token_balance_updates(
                    connection=conn,
                    token_holders=token_holders,
                    last_block=last_block,
                )

                token_updates = await filter_negative_balances(token_updates)
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

                replaced_blocks = list({item['number'] for item in old_chain_fragment})
                for account_state in ether_updates:
                    query = upsert_assets_summary_query(**account_state, blocks_to_replace=replaced_blocks)
                    await conn.execute(query)

                for address in delete_states:
                    query = delete_assets_summary_query(address=address, asset_address=ETHER_ASSET_ADDRESS)
                    await conn.execute(query)

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
        print(j(assets_summary_updates))

        async with self.engine.acquire() as conn:
            async with conn.begin():
                await self.execute(q, [j([block_data]), j(uncles_data), j(transactions_data),
                                       j(receipts_data), j(logs_data), j(accounts_state_data),
                                       j(accounts_base_data), j(internal_txs_data), j(transfers),
                                       j(token_holders_updates), j(wallet_events), j(assets_summary_updates),
                                       j(chain_event)])
