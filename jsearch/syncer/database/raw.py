import logging
from decimal import Decimal

from typing import AsyncGenerator, Any, Dict

from jsearch.common import contracts
from jsearch.common.structs import BlockRange
from jsearch.common.utils import timeit
from jsearch.syncer.structs import TokenHolderBalances, TokenHolderBalance
from .wrapper import DBWrapper

logger = logging.getLogger(__name__)

GENESIS_BLOCK_NUMBER = 0

RAWDB_POOL_SIZE = 1


class RawDB(DBWrapper):
    """
    jSearch RAW db wrapper
    """
    pool_size: int = RAWDB_POOL_SIZE

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

        rows = await self.fetch_all(q, start_id, end_id)

        logger.info(
            "Fetched batch of pending TXs",
            extra={
                'start_id': start_id,
                'end_id': end_id,
                'count': len(rows),
            },
        )

        return rows

    async def get_last_pending_tx_id(self) -> int:
        return await self._get_boundary_pending_tx_id(boundary='max')

    async def get_first_pending_tx_id(self) -> int:
        return await self._get_boundary_pending_tx_id(boundary='min')

    async def _get_boundary_pending_tx_id(self, boundary: str) -> int:
        if boundary not in {'min', 'max'}:
            raise ValueError(f'"boundary" must be either "min" or "max", got "{boundary}"')

        q = f'SELECT {boundary}("id") AS boundary_id FROM "pending_transactions"'

        row = await self.fetch_one(q)

        return row and row['boundary_id'] or 0

    @timeit('[RAW DB] Get parent hash')
    async def get_parent_hash(self, block_hash):
        q = """SELECT fields FROM headers WHERE block_hash=%s"""
        row = await self.fetch_one(q, block_hash)
        return row['fields']['parentHash']

    @timeit('[RAW DB] Get next chain event')
    async def get_next_chain_event(self, block_range: BlockRange, event_id: int, node_id: str):
        params = [event_id, node_id]
        if block_range.end is not None:
            block_cond = """block_number >= %s AND block_number <= %s"""
            params += list(block_range)
        else:
            block_cond = """block_number >= %s"""
            params.append(block_range.start)

        q = f"""SELECT * FROM chain_events WHERE
                    id > %s
                    AND node_id=%s
                    AND {block_cond}
                  ORDER BY id ASC LIMIT 1"""

        return await self.fetch_one(q, *params)

    @timeit('[RAW DB] Get chain splits')
    async def get_chain_splits_for_range(
            self,
            block_range: BlockRange,
            node_id: str
    ) -> AsyncGenerator[Dict[str, Any], None]:
        q = f"""
            SELECT * FROM chain_events
            WHERE
                block_number BETWEEN %s and %s
                AND node_id=%s
                AND "type"='split'
            ORDER BY id ASC
        """
        return self.fetch_all_async(q, block_range.start, block_range.end, node_id)

    @timeit('[RAW DB] Get first chain event')
    async def get_first_chain_event_for_block_range(self, block_range: BlockRange, node_id):
        if block_range.end is not None:
            cond = """block_number >= %s AND block_number <= %s"""
            params = list(block_range)
        else:
            cond = """block_number >= %s"""
            params = [block_range.start]
        params.append(node_id)

        q = f"""
            SELECT * FROM chain_events
            WHERE {cond}  AND node_id=%s
            ORDER BY id ASC LIMIT 1
        """
        return await self.fetch_one(q, *params)

    @timeit('[RAW DB] Is it canonical block query')
    async def is_canonical_block(self, block_hash):
        q = """SELECT id, reinserted FROM reorgs WHERE block_hash=%s ORDER BY id DESC"""

        rows = await self.fetch_all(q, block_hash)

        if len(rows) == 0:
            return True

        if rows[0]['reinserted'] is True:
            return True

        return False

    async def get_header_by_hash(self, block_hash):
        q = """SELECT "block_number", "block_hash", "fields" FROM "headers" WHERE "block_hash"=%s"""
        return await self.fetch_one(q, block_hash)

    async def get_block_accounts(self, block_hash):
        q = """SELECT "id", "block_number", "block_hash", "address", "fields" FROM "accounts" WHERE "block_hash"=%s"""
        return await self.fetch_all(q, block_hash)

    async def get_block_body(self, block_hash):
        q = """SELECT "block_number", "block_hash", "fields" FROM "bodies" WHERE "block_hash"=%s"""
        return await self.fetch_one(q, block_hash)

    async def get_block_receipts(self, block_hash):
        q = """SELECT "block_number", "block_hash", "fields" FROM "receipts" WHERE "block_hash"=%s"""
        return await self.fetch_one(q, block_hash)

    async def get_token_holder_balances(self, block_hash: str) -> TokenHolderBalances:
        query = """
        SELECT
            "block_number",
            "block_hash",
            lower("token_address") as token,
            lower("holder_address") as account,
            "decimals",
            "balance"
        FROM token_holders
        WHERE block_hash = %s;
        """
        rows = await self.fetch_all(query, block_hash)
        rows = [dict(row) for row in rows]
        for row in rows:
            if isinstance(row['balance'], Decimal):
                row['balance'] = int(row['balance'])
        return [TokenHolderBalance(**row) for row in rows]

    async def get_reward(self, block_number, block_hash):
        if block_number == GENESIS_BLOCK_NUMBER:
            return get_reward_for_genesis_block(block_hash)

        q = """SELECT "id", "block_number", "block_hash", "address", "fields" FROM "rewards" WHERE "block_hash"=%s"""
        rows = await self.fetch_all(q, block_hash)

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

        return await self.fetch_all(q, block_hash)


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
