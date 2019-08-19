import logging

from jsearch.common import contracts
from .common import DBWrapper

logger = logging.getLogger(__name__)

GENESIS_BLOCK_NUMBER = 0


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
