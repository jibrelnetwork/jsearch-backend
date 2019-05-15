import json
import logging
from collections import defaultdict

import asyncpgsa
from asyncpg import Connection
from itertools import groupby
from sqlalchemy import select
from sqlalchemy.orm import Query
from typing import DefaultDict
from typing import List, Optional, Dict, Any

from jsearch.api import models
from jsearch.api.database_queries.blocks import (
    get_block_by_hash_query,
    get_block_by_number_query,
    get_last_block_query,
    get_blocks_query,
    get_mined_blocks_query,
    get_block_number_by_hash_query
)
from jsearch.api.database_queries.internal_transactions import get_internal_txs_by_parent, get_internal_txs_by_account
from jsearch.api.database_queries.pending_transactions import get_pending_txs_by_account
from jsearch.api.database_queries.token_transfers import (
    get_token_transfers_by_token,
    get_token_transfers_by_account
)
from jsearch.api.database_queries.transactions import (
    get_tx_by_address,
    get_tx_hashes_by_block_hashes_query,
    get_tx_hashes_by_block_hash_query,
    get_txs_for_events_query)
from jsearch.api.database_queries.uncles import (
    get_uncle_hashes_by_block_hashes_query,
    get_uncle_hashes_by_block_hash_query,
)
from jsearch.api.database_queries.wallet_events import get_wallet_events_query
from jsearch.api.helpers import Tag, fetch_row
from jsearch.api.helpers import fetch
from jsearch.api.structs import BlockchainTip, BlockInfo
from jsearch.common.queries import in_app_distinct
from jsearch.common.tables import blocks_t, chain_splits_t, reorgs_t, wallet_events_t
from jsearch.common.wallet_events import get_event_from_pending_tx
from jsearch.utils import split

logger = logging.getLogger(__name__)

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
        rows = in_app_distinct(rows)

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

            # FAQ: `SELECT DISTINCT` performs two times slower than `SELECT`, so use
            # `in_app_distinct` instead.
            rows = in_app_distinct([dict(row) for row in rows])

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
            txs = in_app_distinct(txs)

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

    async def get_latest_block_info(self) -> Optional[BlockInfo]:
        last_block_query = get_last_block_query()
        async with self.pool.acquire() as conn:
            last_block = await fetch_row(conn, query=last_block_query)

        if last_block is not None:
            return BlockInfo(
                hash=last_block['hash'],
                number=last_block['number']
            )

    async def get_block_number(self, block_hash: str) -> Optional[BlockInfo]:
        query = get_block_number_by_hash_query(block_hash)
        async with self.pool.acquire() as conn:
            block = await fetch_row(conn, query=query)

        if block is not None:
            return BlockInfo(
                hash=block_hash,
                number=block['number']
            )

    async def get_blockchain_tip(self,
                                 last_block: BlockInfo,
                                 tip: Optional[BlockInfo]) -> Optional[BlockchainTip]:
        """
        Return status of client's last known block
        """
        is_in_fork = False
        last_unchanged = None
        if tip:
            split_query = select([chain_splits_t.c.common_block_number]).where(
                chain_splits_t.c.id == select([reorgs_t.c.split_id]).where(reorgs_t.c.block_hash == tip.hash)
            )

            async with self.pool.acquire() as conn:
                chain_split = await fetch_row(conn, query=split_query)

            is_in_fork = chain_split is not None
            common_block_number = chain_split and chain_split['common_block_number']
            if is_in_fork and common_block_number is not None:
                last_unchanged = common_block_number

        return BlockchainTip(
            tip_hash=tip and tip.hash,
            tip_number=tip and tip.number,
            last_hash=last_block.hash,
            last_number=last_block.number,
            is_in_fork=is_in_fork,
            last_unchanged_block=last_unchanged
        )

    async def get_wallet_events(self,
                                address: str,
                                from_block: int,
                                until_block: int,
                                limit: int,
                                order: str,
                                offset: int) -> Dict[str, List[Dict[str, Any]]]:
        query = get_wallet_events_query(
            address=address,
            from_block=from_block,
            until_block=until_block,
            limit=limit,
            offset=offset,
            order=order
        )
        async with self.pool.acquire() as connection:
            rows = await fetch(connection, query)

        events_per_tx = defaultdict(list)
        for item in rows:
            tx_hash = item['tx_hash']
            data = models.WalletEvent(**item).to_dict()

            events_per_tx[tx_hash].append(data)

        return events_per_tx

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

    async def get_wallet_events_transactions(self,
                                             address: str,
                                             from_block: int,
                                             until_block: int,
                                             limit: int,
                                             order: str,
                                             offset: int) -> List[Dict[str, Any]]:

        events_query = get_wallet_events_query(
            address=address,
            from_block=from_block,
            until_block=until_block,
            limit=limit,
            offset=offset,
            order=order,
            columns=[wallet_events_t.c.tx_hash]
        )
        query = get_txs_for_events_query(events_query, order)

        async with self.pool.acquire() as conn:
            rows = await fetch(conn, query)
            distinct_set = set()
            transactions = []
            for row in rows:
                row = dict(row)
                distinct_key = tuple(row.values())
                if distinct_key in distinct_set:
                    continue

                distinct_set.add(distinct_key)
                transactions.append(models.Transaction(**row).to_dict())
        return transactions

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

    async def get_internal_transactions(self,
                                        parent_tx_hash: str,
                                        limit: int,
                                        offset: int,
                                        order: str):

        query = get_internal_txs_by_parent(parent_tx_hash, order)
        query = query.limit(limit)
        query = query.offset(offset)

        rows = await fetch(self.pool, query)
        internal_txs = [models.InternalTransaction(**r) for r in rows]

        return internal_txs

    async def get_account_internal_transactions(self,
                                                account: str,
                                                limit: int,
                                                offset: int,
                                                order: str):

        query = get_internal_txs_by_account(account, order)
        query = query.limit(limit)
        query = query.offset(offset)

        rows = await fetch(self.pool, query)
        internal_txs = [models.InternalTransaction(**r) for r in rows]

        return internal_txs

    async def get_account_pending_transactions(self,
                                               account: str,
                                               order: str,
                                               limit: Optional[int] = None,
                                               offset: Optional[int] = None):

        query = get_pending_txs_by_account(account, order)

        if limit:
            query = query.limit(limit)

        if offset:
            query = query.offset(offset)

        rows = await fetch(self.pool, query)
        return [models.PendingTransaction(**r) for r in rows]

    async def get_account_pending_events(self,
                                         account: str,
                                         order: str,
                                         limit: Optional[int] = None,
                                         offset: Optional[int] = None) -> List[Dict[str, Any]]:

        query = get_pending_txs_by_account(account, order)

        if limit:
            query = query.limit(limit)

        if offset:
            query = query.offset(offset)

        rows = await fetch(self.pool, query)

        result = []
        for tx in rows:
            event = get_event_from_pending_tx(address=account, pending_tx=tx)
            tx_data = {
                'rootTxData': models.PendingTransaction(**tx).to_dict(),
                'events': models.WalletEvent(**event).to_dict()
            }
            result.append(tx_data)

        return result
