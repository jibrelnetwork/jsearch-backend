import json
import logging
from collections import defaultdict
from itertools import groupby
from typing import DefaultDict
from typing import List, Optional, Dict, Any

import asyncpgsa
from asyncpg import Connection
from sqlalchemy.orm import Query

from jsearch.api import models
from jsearch.api.database_queries.blocks import (
    get_block_by_hash_query,
    get_block_by_number_query,
    get_blocks_query,
    get_last_block_query,
    get_mined_blocks_query,
)
from jsearch.api.database_queries.token_transfers import (
    get_token_transfers_by_token,
    get_token_transfers_by_account
)
from jsearch.api.database_queries.transactions import (
    get_tx_by_address,
    get_tx_hashes_by_block_hash_query,
    get_tx_hashes_by_block_hashes_query
)
from jsearch.api.database_queries.uncles import (
    get_uncle_hashes_by_block_hash_query,
    get_uncle_hashes_by_block_hashes_query
)
from jsearch.api.helpers import Tag
from jsearch.api.helpers import fetch
from jsearch.common.queries import in_app_distinct
from jsearch.common.tables import blocks_t
from jsearch.utils import split

log = logging.getLogger(__name__)

DEFAULT_ACCOUNT_TRANSACTIONS_LIMIT = 20
MAX_ACCOUNT_TRANSACTIONS_LIMIT = 200

BLOCKS_IN_QUERY = 10


def _group_by_block(items: List[Dict[str, Any]]) -> DefaultDict[str, List[Dict[str, Any]]]:
    items_by_block = defaultdict(list)
    for item in items:
        item_hash = item['hash']
        block_hash = item['block_hash']

        items_by_block[block_hash].append(item_hash)
    return items_by_block


async def _fetch_blocks(connection: Connection, query: Query) -> List[Dict[str, Any]]:
    """
    fetch blocks with included transactions and uncles hashes.

    HACK: to get included transactions ans hashes we make
        a query for each 10 block hashes. Because if we do
        a query with `in` clause contains more then 10 entities -
        query will be very slow.

    """
    rows = await fetch(connection=connection, query=query)

    block_hashes = [row['hash'] for row in rows]

    txs_by_block = {}
    uncles_by_block = {}

    for hashes in split(block_hashes, BLOCKS_IN_QUERY):
        uncles_query = get_uncle_hashes_by_block_hashes_query(hashes)
        uncles = await fetch(connection, uncles_query)

        uncles_by_block.update(_group_by_block(uncles))

        tx_query = get_tx_hashes_by_block_hashes_query(hashes)
        txs = await fetch(connection, tx_query)
        txs_by_block.update(_group_by_block(txs))

    for row in rows:
        block_hash = row['hash']

        row_txs = txs_by_block.get(block_hash) or []
        row_uncles = uncles_by_block.get(block_hash) or []

        row.update({
            'static_reward': int(row['static_reward']),
            'uncle_inclusion_reward': int(row['uncle_inclusion_reward']),
            'tx_fees': int(row['tx_fees']),
            'transactions': row_txs,
            'uncles': row_uncles
        })
    return rows


def _rows_to_token_transfers(rows: List[Dict[str, Any]]) -> List[models.TokenTransfer]:
    token_transfers = list()

    for row in rows:
        del row['transaction_index']
        del row['log_index']
        del row['block_number']
        del row['block_hash']

        token_transfers.append(models.TokenTransfer(**row))

    return token_transfers


class Storage:

    def __init__(self, pool):
        self.pool = pool

    async def get_account(self, address, tag):
        """
        Get account info by address
        """
        if tag.is_hash():
            query = """
                SELECT block_number, block_hash, address, nonce, balance FROM accounts_state
                WHERE address=$1
                    AND block_number<=(SELECT number FROM blocks WHERE hash=$2)
                ORDER BY block_number DESC LIMIT 1;
            """
        elif tag.is_number():
            query = """
                SELECT block_number, block_hash, address, nonce, balance FROM accounts_state
                WHERE address=$1 AND block_number<=$2
                ORDER BY block_number DESC LIMIT 1;
            """
        else:
            query = """
                SELECT "block_number", "block_hash", "address", "nonce", "balance" FROM accounts_state
                WHERE address=$1 ORDER BY block_number DESC LIMIT 1;
            """
        async with self.pool.acquire() as conn:
            if tag.is_latest():
                state_row = await conn.fetchrow(query, address)
            else:
                state_row = await conn.fetchrow(query, address, tag.value)

            if state_row is None:
                return None

            state_row = dict(state_row)
            state_row['balance'] = int(state_row['balance'])

            query = """
                SELECT address, code, code_hash FROM accounts_base
                WHERE address=$1 LIMIT 1;
            """
            base_row = dict(await conn.fetchrow(query, address))

            row = {}
            row.update(state_row)
            row.update(base_row)

            return models.Account(**row)

    async def get_account_transactions(self, address, limit, offset, order):
        limit = min(limit, MAX_ACCOUNT_TRANSACTIONS_LIMIT)

        query = get_tx_by_address(address.lower(), order)
        query = query.limit(limit)
        query = query.offset(offset)

        rows = await fetch(self.pool, query)
        txs = [models.Transaction(**r) for r in rows]

        return txs

    async def get_block_transactions(self, tag):
        fields = models.Transaction.select_fields()

        if tag.is_hash():
            query = f"SELECT {fields} FROM transactions WHERE block_hash=$1 AND is_forked=false " \
                    f"ORDER BY transaction_index;"
        elif tag.is_number():
            query = f"SELECT {fields} FROM transactions WHERE block_number=$1 AND is_forked=false " \
                    f"ORDER BY transaction_index;"
        else:
            query = f"""
                SELECT {fields} FROM transactions
                WHERE block_number=(SELECT max(number) FROM blocks) AND is_forked=false ORDER BY transaction_index;
        """

        async with self.pool.acquire() as conn:
            if tag.is_latest():
                rows = await conn.fetch(query)
            else:
                rows = await conn.fetch(query, tag.value)
            rows = [dict(r) for r in rows]
            return [models.Transaction(**r) for r in rows]

    async def get_block(self, tag: Tag):
        if tag.is_hash():
            query = get_block_by_hash_query(block_hash=tag.value)

        elif tag.is_number():
            query = get_block_by_number_query(number=tag.value)

        else:
            query = get_last_block_query()

        async with self.pool.acquire() as conn:
            query, params = asyncpgsa.compile_query(query)
            row = await conn.fetchrow(query, *params)

            if row is None:
                return None

            data = dict(row)
            data.update(
                tx_fees=int(data['tx_fees']),
                static_reward=int(data['static_reward']),
                uncle_inclusion_reward=int(data['uncle_inclusion_reward']),
            )

            tx_query = get_tx_hashes_by_block_hash_query(block_hash=data['hash'])
            tx_query, params = asyncpgsa.compile_query(tx_query)
            txs = await conn.fetch(tx_query, *params)

            data['transactions'] = [tx['hash'] for tx in txs]

            uncles_query = get_uncle_hashes_by_block_hash_query(block_hash=data['hash'])
            uncles_query, params = asyncpgsa.compile_query(uncles_query)
            uncles = await conn.fetch(uncles_query, *params)

            data['uncles'] = [uncle['hash'] for uncle in uncles]

            return models.Block(**data)

    async def get_blocks(self, limit, offset, order):
        assert order in {'asc', 'desc'}, 'Invalid order value: {}'.format(order)

        query = get_blocks_query(limit=limit, offset=offset, order=[blocks_t.c.number], direction=order)
        async with self.pool.acquire() as connection:
            rows = await _fetch_blocks(connection, query)

        return [models.Block(**row) for row in rows]

    async def get_account_mined_blocks(self, address, limit, offset, order):
        assert order in {'asc', 'desc'}, 'Invalid order value: {}'.format(order)
        query = get_mined_blocks_query(
            miner=address,
            limit=limit,
            offset=offset,
            order=[blocks_t.c.number],
            direction=order
        )
        async with self.pool.acquire() as connection:
            rows = await _fetch_blocks(connection, query)

        return [models.Block(**row) for row in rows]

    async def get_uncle(self, tag):
        if tag.is_hash():
            query = "SELECT * FROM uncles WHERE hash=$1"
        elif tag.is_number():
            query = "SELECT * FROM uncles WHERE number=$1"
        else:
            query = "SELECT * FROM uncles WHERE number=(SELECT max(number) FROM uncles)"

        async with self.pool.acquire() as conn:
            if tag.is_latest():
                row = await conn.fetchrow(query)
            else:
                row = await conn.fetchrow(query, tag.value)
            if row is None:
                return None
            data = dict(row)
            del data['block_hash']
            del data['is_forked']
            data['reward'] = int(data['reward'])
            return models.Uncle(**data)

    async def get_uncles(self, limit, offset, order):
        assert order in {'asc', 'desc'}, 'Invalid order value: {}'.format(order)
        query = f"""SELECT * FROM uncles ORDER BY number {order} LIMIT $1 OFFSET $2"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, limit, offset)
            rows = [dict(r) for r in rows]
            for r in rows:
                del r['block_hash']
                del r['is_forked']
                r['reward'] = int(r['reward'])
            return [models.Uncle(**row) for row in rows]

    async def get_account_mined_uncles(self, address, limit, offset, order):
        assert order in {'asc', 'desc'}, 'Invalid order value: {}'.format(order)
        query = f"""SELECT * FROM uncles WHERE miner=$1 ORDER BY number {order} LIMIT $2 OFFSET $3"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, address, limit, offset)
            rows = [dict(r) for r in rows]
            for r in rows:
                del r['block_hash']
                r['reward'] = int(r['reward'])
            return [models.Uncle(**row) for row in rows]

    async def get_block_uncles(self, tag):
        if tag.is_hash():
            query = "SELECT * FROM uncles WHERE block_hash=$1"
        elif tag.is_number():
            query = "SELECT * FROM uncles WHERE block_number=$1"
        else:
            query = "SELECT * FROM uncles WHERE block_number=(SELECT max(number) FROM blocks)"

        async with self.pool.acquire() as conn:
            if tag.is_latest():
                rows = await conn.fetch(query)
            else:
                rows = await conn.fetch(query, tag.value)
            rows = [dict(r) for r in rows]
            for r in rows:
                del r['block_hash']
                del r['is_forked']
                r['reward'] = int(r['reward'])
            return [models.Uncle(**r) for r in rows]

    async def get_transaction(self, tx_hash):
        fields = models.Transaction.select_fields()
        query = f"SELECT {fields} FROM transactions WHERE hash=$1"

        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, tx_hash)
            if row is None:
                return None
            row = dict(row)
            return models.Transaction(**row)

    async def get_receipt(self, tx_hash):
        query = "SELECT * FROM receipts WHERE transaction_hash=$1"
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, tx_hash)
            if row is None:
                return None
            row = dict(row)
            del row['is_forked']
            row['logs'] = await self.get_logs(row['transaction_hash'])
            return models.Receipt(**row)

    async def get_logs(self, tx_hash):
        fields = models.Log.select_fields()
        query = f"SELECT {fields} FROM logs WHERE transaction_hash=$1 ORDER BY log_index"

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, tx_hash)
            return [models.Log(**r) for r in rows]

    async def get_accounts_balances(self, addresses):
        query = """SELECT a.address, a.balance FROM accounts_state a
                    INNER JOIN (SELECT address, max(block_number) bn FROM accounts_state
                                 WHERE address = any($1::text[]) GROUP BY address) gn
                    ON a.address=gn.address AND a.block_number=gn.bn;"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, addresses)
            addr_map = {r['address']: r for r in rows}
            return [models.Balance(balance=int(addr_map[a]['balance']), address=addr_map[a]['address'])
                    for a in addresses if a in addr_map]

    async def get_tokens_transfers(self,
                                   address: str,
                                   limit: int,
                                   offset: int,
                                   order: str) -> List[models.TokenTransfer]:
        # HACK: There're 2 times more entries due to denormalization, see
        # `log_to_transfers`. Because of this, `offset` and `limit` should be
        # multiplied first and rows should be deduped second.
        offset *= 2
        limit *= 2

        query = get_token_transfers_by_token(address.lower(), order)
        query = query.limit(limit)
        query = query.offset(offset)

        rows = await fetch(self.pool, query)
        # FAQ: `SELECT DISTINCT` performs two times slower than `SELECT`, so use
        # `in_app_distinct` instead.
        rows_distinct = in_app_distinct(rows)

        return _rows_to_token_transfers(rows_distinct)

    async def get_account_tokens_transfers(self,
                                           address: str,
                                           limit: int,
                                           offset: int,
                                           order: str) -> List[models.TokenTransfer]:
        query = get_token_transfers_by_account(address.lower(), order)
        query = query.limit(limit)
        query = query.offset(offset)

        rows = await fetch(self.pool, query)
        transfers = _rows_to_token_transfers(rows)

        return transfers

    async def get_contact_creation_code(self, address: str) -> str:
        query = """
        SELECT input FROM transactions t
          INNER JOIN receipts r
           ON t.hash = r.transaction_hash
           AND r.contract_address = $1
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, address)
        return row['input']

    async def get_tokens_holders(self, address: str, limit: int, offset: int, order: str) \
            -> List[models.TokenHolder]:
        assert order in {'asc', 'desc'}, 'Invalid order value: {}'.format(order)
        query = f"""
        SELECT account_address, token_address, balance, decimals
        FROM token_holders
        WHERE token_address=$1
        ORDER BY balance {order} LIMIT $2 OFFSET $3;
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, address, limit, offset)
            return [models.TokenHolder(**r) for r in rows]

    async def get_account_token_balance(self, account_address: str, token_address: str) \
            -> List[models.TokenHolder]:
        query = """
        SELECT account_address, token_address, balance, decimals
        FROM token_holders
        WHERE account_address=$1 AND token_address=$2
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, account_address, token_address)
            return models.TokenHolder(**row) if row else None

    async def get_blockchain_tip_status(self, last_known_block_hash: str):
        """
        Retruns status of client's last known block
        """
        split_query = """SELECT common_block_number FROM chain_splits
                            WHERE id=(SELECT  split_id FROM  reorgs where block_hash=$1)"""
        block_query = "SELECT number FROM blocks WHERE hash=$1"

        async with self.pool.acquire() as conn:
            split = await conn.fetchrow(split_query, last_known_block_hash)
            block = await conn.fetchrow(block_query, last_known_block_hash)

        if block is None:
            return None

        if split is None:
            result = {
                'blockchainTip': {
                    'blockHash': last_known_block_hash,
                    'blockNumber': block['number']
                },
                'forkData': {
                    'isInFork': False,
                    'lastUnchangedBlock': block['number']
                }
            }
        else:
            result = {
                'blockchainTip': {
                    'blockHash': last_known_block_hash,
                    'blockNumber': block['number']
                },
                'forkData': {
                    'isInFork': True,
                    'lastUnchangedBlock': split['common_block_number']
                }
            }
        return result

    async def get_wallet_assets_transfers(self, addresses: List[str], limit: int, offset: int,
                                          assets: Optional[List[str]] = None) -> List:

        if assets:
            assets_filter = " AND asset_address = ANY($2::varchar[]) "
            limit_stmt = "LIMIT $3 OFFSET $4 "
        else:
            assets_filter = ""
            limit_stmt = "LIMIT $2 OFFSET $3 "

        query = f"""SELECT * FROM assets_transfers
                        WHERE address = ANY($1::varchar[]) {assets_filter} AND is_forked=false
                        ORDER BY ordering DESC
                        {limit_stmt}"""
        async with self.pool.acquire() as conn:
            if assets:
                rows = await conn.fetch(query, addresses, assets, limit, offset)
            else:
                rows = await conn.fetch(query, addresses, limit, offset)

            items = []
            for row in rows:
                t = dict(row)
                t['tx_data'] = json.loads(t['tx_data'])
                t['amount'] = str(t['value'] / 10 ** t['decimals'])
                items.append(t)
            return [models.AssetTransfer(**t) for t in items]

    async def get_wallet_transactions(self, address: str, limit: int, offset: int) -> List:
        offset *= 2
        limit *= 2
        query = """SELECT * FROM transactions
                        WHERE address = $1
                        ORDER BY block_number, transaction_index DESC
                        LIMIT $2 OFFSET $3 """

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, address, limit, offset)
            distinct_set = set()
            transactions = []
            for row in rows:
                row = dict(row)
                row.pop('address')
                distinct_key = tuple(row.values())
                if distinct_key in distinct_set:
                    continue
                distinct_set.add(distinct_key)
                transactions.append(models.Transaction(**row))
        return transactions

    async def get_wallet_assets_summary(self, addresses: List[str], limit: int, offset: int,
                                        assets: Optional[List[str]] = None):
        if assets:
            assets_filter = " AND asset_address = ANY($2::varchar[]) "
            limit_stmt = "LIMIT $3 OFFSET $4 "
        else:
            assets_filter = ""
            limit_stmt = "LIMIT $2 OFFSET $3 "

        query = f"""SELECT * FROM assets_summary
                        WHERE address = ANY($1::varchar[])
                        {assets_filter}
                        ORDER BY address, asset_address ASC
                        {limit_stmt}"""

        async with self.pool.acquire() as conn:
            if assets:
                rows = await conn.fetch(query, addresses, assets, limit, offset)
            else:
                rows = await conn.fetch(query, addresses, limit, offset)
            addr_map = {k: list(g) for k, g in groupby(rows, lambda r: r['address'])}
            summary = []
            for addr in addresses:
                assets_summary = []
                nonce = 0
                for row in addr_map.get(addr, []):
                    assets_summary.append({
                        'balance': float(row['value'] / 10 ** row['decimals']),
                        'address': row['asset_address'],
                        'transfersNumber': row['tx_number'],
                    })
                    nonce = row['nonce']
                item = {
                    'assetsSummary': assets_summary,
                    'address': addr,
                    'outgoingTransactionsNumber': nonce,
                }
                summary.append(item)
            return summary

    async def get_nonce(self, address):
        """
        Get account nonce
        """

        query = """
            SELECT "nonce" FROM accounts_state
            WHERE address=$1 ORDER BY block_number DESC LIMIT 1;
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, address)
            if row:
                return row['nonce']
            return 0
