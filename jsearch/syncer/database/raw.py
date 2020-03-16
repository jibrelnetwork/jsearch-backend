import logging
from decimal import Decimal
from typing import AsyncGenerator, Any, Dict, List

from jsearch.api.helpers import ChainEvent
from jsearch.common import contracts
from jsearch.common.db import DBWrapper
from jsearch.common.structs import BlockRange
from jsearch.common.utils import timeit
from jsearch.syncer.structs import TokenHolderBalances, TokenHolderBalance, NodeState
from jsearch.typing import AnyDicts

logger = logging.getLogger(__name__)

GENESIS_BLOCK_NUMBER = 0

RAWDB_POOL_SIZE = 2


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
        return await self.iterate_by(q, block_range.start, block_range.end, node_id)

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

    @timeit('[RAW DB] Get insert chain event')
    async def get_insert_chain_event_by_block_hash(self, block_hash: str, node_id: str) -> Dict[str, Any]:
        query = """
        SELECT * FROM chain_events
        WHERE
            block_hash = %s
            AND node_id = %s
            AND "type" = 'created'
        LIMIT 1;
        """
        return await self.fetch_one(query, block_hash, node_id)

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

    async def get_token_descriptions(self, block_hash: str) -> AnyDicts:
        query = """
        SELECT
            "block_number",
            "block_hash",
            lower("token") as token,
            "total_supply"
        FROM token_descriptions
        WHERE block_hash = %s;
        """
        rows = await self.fetch_all(query, block_hash)
        results = [dict(row) for row in rows]
        for item in results:
            item['total_supply'] = int(item['total_supply'])
        return results

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

    async def get_nodes(self, block_range: BlockRange) -> List[NodeState]:
        assert block_range.is_closed, 'Do not query in open range'
        assert len(block_range) < 10000, 'Do not query too many blocks'

        q = """
        SELECT node_id, count(id) as events
        FROM chain_events
        WHERE block_number BETWEEN %s and %s
        GROUP BY node_id;
        """
        results = await self.fetch_all(q, block_range.start, block_range.end)
        if results:
            return [NodeState(id=item['node_id'], events=item['events']) for item in results]
        return list()

    async def get_nodes_for_block(self, block_number: int, exclude_node: str) -> List[str]:
        q = """
        SELECT node_id
        FROM chain_events
        WHERE block_number = %s and node_id != %s
        GROUP BY node_id;
        """
        return [item['node_id'] for item in await self.fetch_all(q, block_number, exclude_node)]

    async def get_chain_events_for_range(
            self,
            node_id: str,
            block_range: BlockRange,
            event_type: str = ChainEvent.INSERT
    ) -> List[Dict[str, Any]]:
        assert block_range.is_closed, 'Do not query in open range'
        assert len(block_range) < 10000, 'Do not query too many blocks'

        q = f"""
        SELECT block_number, block_hash, parent_block_hash
        FROM chain_events
        WHERE
            block_number BETWEEN %s and %s
            and node_id = %s
            and type = '{event_type}'
        ORDER BY block_number
        """
        return await self.fetch_all(q, block_range.start, block_range.end, node_id)

    async def get_common_block_number(
            self,
            node_left: str,
            node_right: str,
            block_range: BlockRange,
    ) -> int:
        chain_left = await self.get_chain_events_for_range(node_left, block_range)
        chain_right = await self.get_chain_events_for_range(node_right, block_range)

        chain_left_map = {item['block_hash']: item for item in chain_left}

        links = [event for event in chain_right if event['parent_block_hash'] in chain_left_map]
        try:
            block = sorted(links, key=lambda event: event['block_number'])[-1]
        except IndexError:
            common_block = None
        else:
            common_block = block['block_number']

        if common_block is None:
            raise ValueError(f'No common block between {node_left} and {node_right}')

        return common_block


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
