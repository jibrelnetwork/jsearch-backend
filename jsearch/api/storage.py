import json
import logging
from collections import defaultdict, OrderedDict

import asyncpgsa
from itertools import groupby
from sqlalchemy import select
from typing import DefaultDict, Tuple
from typing import List, Optional, Dict, Any

from jsearch.api import models
from jsearch.api.database_queries.assets_summary import get_assets_summary_query
from jsearch.api.database_queries.blocks import (
    get_block_by_hash_query,
    get_block_by_number_query,
    get_last_block_query,
    get_mined_blocks_query,
    get_block_number_by_hash_query,
    ORDER_SCHEME_BY_TIMESTAMP,
    get_blocks_by_timestamp_query,
    ORDER_SCHEME_BY_NUMBER,
    get_blocks_by_number_query
)
from jsearch.api.database_queries.internal_transactions import get_internal_txs_by_parent, get_internal_txs_by_account
from jsearch.api.database_queries.logs import get_logs_by_address_query
from jsearch.api.database_queries.pending_transactions import get_pending_txs_by_account
from jsearch.api.database_queries.token_transfers import (
    get_token_transfers_by_token,
    get_token_transfers_by_account
)
from jsearch.api.database_queries.transactions import (
    get_txs_for_events_query,
    get_tx_by_hash,
    get_tx_by_address_and_block_query,
    get_tx_by_address_and_timestamp_query
)
from jsearch.api.database_queries.wallet_events import get_wallet_events_query
from jsearch.api.helpers import Tag, fetch_row
from jsearch.api.helpers import fetch
from jsearch.api.ordering import Ordering
from jsearch.api.structs import AddressesSummary, AssetSummary, AddressSummary, BlockchainTip, BlockInfo
from jsearch.common.queries import in_app_distinct
from jsearch.common.tables import blocks_t, chain_splits_t, reorgs_t, wallet_events_t
from jsearch.common.wallet_events import get_event_from_pending_tx
from jsearch.typing import LastAffectedBlock

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


def _rows_to_token_transfers(rows: List[Dict[str, Any]]) -> List[models.TokenTransfer]:
    token_transfers = list()

    for row in rows:
        token_transfers.append(models.TokenTransfer(**row))

    return token_transfers


class Storage:

    def __init__(self, pool):
        self.pool = pool

    async def get_account(self, address, tag) -> Tuple[Optional[models.Account], Optional[LastAffectedBlock]]:
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
                return None, None

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

            row['code'] = '0x' + row['code']
            row['code_hash'] = '0x' + row['code_hash']

        account = models.Account(**row)
        last_affected_block = state_row['block_number']

        return account, last_affected_block

    async def get_account_transactions(
            self,
            address: str,
            limit: int,
            ordering: Ordering,
            block_number: int,
            timestamp: int,
            tx_index: Optional[int] = None
    ) -> Tuple[List[models.Transaction], Optional[LastAffectedBlock]]:

        limit = min(limit, MAX_ACCOUNT_TRANSACTIONS_LIMIT)

        if ordering.scheme == ORDER_SCHEME_BY_NUMBER:
            query = get_tx_by_address_and_block_query(address, block_number, ordering, tx_index)
        else:
            query = get_tx_by_address_and_timestamp_query(address, timestamp, ordering, tx_index)

        # Notes: syncer writes txs to main db with denormalization (x2 records per transaction)
        query = query.limit(limit * 2)

        async with self.pool.acquire() as connection:
            rows = await fetch(connection, query)

        rows = in_app_distinct(rows)[:limit]

        txs = [models.Transaction(**r) for r in rows]
        last_affected_block = max((r['block_number'] for r in rows), default=None)

        return txs, last_affected_block

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

            uncles = row['uncles'] or []
            if uncles:
                uncles = json.loads(uncles)

            txs = row['transactions'] or []
            if txs:
                txs = json.loads(txs)

            # TODO: int transformation should do serializer
            data = dict(row)
            data.update(
                transactions=txs,
                uncles=uncles,
                tx_fees=int(data['tx_fees']),
                static_reward=int(data['static_reward']),
                uncle_inclusion_reward=int(data['uncle_inclusion_reward']),
            )

            return models.Block(**data)

    async def get_blocks(
            self,
            limit: int,
            order: Ordering,
            number: Optional[int] = None,
            timestamp: Optional[int] = None,
    ) -> Tuple[List[models.Block], Optional[LastAffectedBlock]]:
        if order.scheme == ORDER_SCHEME_BY_TIMESTAMP:
            query = get_blocks_by_timestamp_query(limit=limit, timestamp=timestamp, order=order)

        elif order.scheme == ORDER_SCHEME_BY_NUMBER:
            query = get_blocks_by_number_query(limit, number=number, order=order)

        else:
            raise ValueError('Invalid scheme: {scheme}')

        async with self.pool.acquire() as connection:
            rows = await fetch(connection=connection, query=query)

            for row in rows:
                uncles = row.get('uncles')
                if uncles:
                    uncles = json.loads(uncles)

                txs = row.get('transactions')
                if txs:
                    txs = json.loads(txs)

                row.update({
                    'uncles': uncles,
                    'transactions': txs,
                    'static_reward': int(row['static_reward']),
                    'uncle_inclusion_reward': int(row['uncle_inclusion_reward']),
                    'tx_fees': int(row['tx_fees']),
                })

        blocks = [models.Block(**row) for row in rows]
        last_affected_block = max((r['number'] for r in rows), default=None)

        return blocks, last_affected_block

    async def get_account_mined_blocks(
            self,
            address,
            limit,
            offset,
            order
    ) -> Tuple[List[models.Block], Optional[LastAffectedBlock]]:
        assert order in {'asc', 'desc'}, 'Invalid order value: {}'.format(order)
        query = get_mined_blocks_query(
            miner=address,
            limit=limit,
            offset=offset,
            order=[blocks_t.c.number],
            direction=order
        )
        async with self.pool.acquire() as connection:
            rows = await fetch(connection=connection, query=query)
            for row in rows:
                uncles = row.get('uncles')
                if uncles:
                    uncles = json.loads(uncles)

                txs = row.get('transactions')
                if txs:
                    txs = json.loads(txs)

                row.update({
                    'uncles': uncles,
                    'transactions': txs,
                    'static_reward': int(row['static_reward']),
                    'uncle_inclusion_reward': int(row['uncle_inclusion_reward']),
                    'tx_fees': int(row['tx_fees']),
                })

        blocks = [models.Block(**row) for row in rows]
        last_affected_block = max((r['number'] for r in rows), default=None)

        return blocks, last_affected_block

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

    async def get_uncles(self, limit, offset, order) -> Tuple[List[models.Uncle], Optional[LastAffectedBlock]]:
        assert order in {'asc', 'desc'}, 'Invalid order value: {}'.format(order)
        query = f"""SELECT * FROM uncles ORDER BY number {order} LIMIT $1 OFFSET $2"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, limit, offset)
            rows = [dict(r) for r in rows]
            for r in rows:
                del r['block_hash']
                del r['is_forked']
                r['reward'] = int(r['reward'])

        uncles = [models.Uncle(**row) for row in rows]
        last_affected_block = max((r['block_number'] for r in rows), default=None)

        return uncles, last_affected_block

    async def get_account_mined_uncles(
            self, address, limit, offset, order
    ) -> Tuple[List[models.Uncle], Optional[LastAffectedBlock]]:
        assert order in {'asc', 'desc'}, 'Invalid order value: {}'.format(order)
        query = f"""SELECT * FROM uncles WHERE miner=$1 ORDER BY number {order} LIMIT $2 OFFSET $3"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, address, limit, offset)
            rows = [dict(r) for r in rows]
            for r in rows:
                del r['block_hash']
                r['reward'] = int(r['reward'])

        uncles = [models.Uncle(**row) for row in rows]
        last_affected_block = max((r['block_number'] for r in rows), default=None)

        return uncles, last_affected_block

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
        query = get_tx_by_hash(tx_hash)
        async with self.pool.acquire() as conn:
            row = await fetch_row(conn, query)
            if row:
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

    async def get_logs(self, tx_hash: str) -> List[models.Log]:
        fields = models.Log.select_fields()
        query = f"SELECT {fields} FROM logs WHERE transaction_hash=$1 ORDER BY log_index"

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, tx_hash)
            return [models.Log(**r) for r in rows]

    async def get_account_logs(self,
                               address: str,
                               block_from: int,
                               block_until: int,
                               order: str,
                               limit: int,
                               offset: int) -> Tuple[List[models.Log], Optional[LastAffectedBlock]]:
        query = get_logs_by_address_query(address, order, limit, offset, block_from, block_until)

        async with self.pool.acquire() as conn:
            rows = await fetch(conn, query)

        logs = [models.Log(**r) for r in rows]
        last_affected_block = max((r['block_number'] for r in rows), default=None)

        return logs, last_affected_block

    async def get_accounts_balances(self, addresses) -> Tuple[List[models.Balance], Optional[LastAffectedBlock]]:
        query = """SELECT a.address, a.balance, a.block_number FROM accounts_state a
                    INNER JOIN (SELECT address, max(block_number) bn FROM accounts_state
                                 WHERE address = any($1::text[]) GROUP BY address) gn
                    ON a.address=gn.address AND a.block_number=gn.bn;"""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, addresses)
            addr_map = {r['address']: r for r in rows}

        balances = [
            models.Balance(balance=int(addr_map[a]['balance']), address=addr_map[a]['address'])
            for a in addresses if a in addr_map
        ]
        last_affected_block = max((r['block_number'] for r in rows), default=None)

        return balances, last_affected_block

    async def get_tokens_transfers(self,
                                   address: str,
                                   limit: int,
                                   offset: int,
                                   order: str) -> Tuple[List[models.TokenTransfer], Optional[LastAffectedBlock]]:
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

        transfers = _rows_to_token_transfers(rows_distinct)
        last_affected_block = max((r['block_number'] for r in rows), default=None)

        return transfers, last_affected_block

    async def get_account_tokens_transfers(
            self,
            address: str,
            limit: int,
            offset: int,
            order: str
    ) -> Tuple[List[models.TokenTransfer], Optional[LastAffectedBlock]]:

        query = get_token_transfers_by_account(address.lower(), order)
        query = query.limit(limit)
        query = query.offset(offset)

        rows = await fetch(self.pool, query)
        transfers = _rows_to_token_transfers(rows)
        last_affected_block = max((r['block_number'] for r in rows), default=None)

        return transfers, last_affected_block

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
            -> Tuple[List[models.TokenHolder], Optional[LastAffectedBlock]]:
        assert order in {'asc', 'desc'}, 'Invalid order value: {}'.format(order)
        query = f"""
        SELECT account_address, token_address, balance, decimals, block_number
        FROM token_holders
        WHERE token_address=$1
        ORDER BY balance {order} LIMIT $2 OFFSET $3;
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, address, limit, offset)

        holders = [models.TokenHolder(**r) for r in rows]
        last_affected_block = max((r['block_number'] for r in rows), default=None)

        return holders, last_affected_block

    async def get_account_token_balance(self, account_address: str, token_address: str) \
            -> Tuple[Optional[models.TokenHolder], Optional[LastAffectedBlock]]:
        query = """
        SELECT account_address, token_address, balance, decimals, block_number
        FROM token_holders
        WHERE account_address=$1 AND token_address=$2
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, account_address, token_address)

        if not row:
            return None, None

        holder = models.TokenHolder(**row)
        last_affected_block = row['block_number']

        return holder, last_affected_block

    async def get_latest_block_info(self) -> Optional[BlockInfo]:
        last_block_query = get_last_block_query()
        async with self.pool.acquire() as conn:
            last_block = await fetch_row(conn, query=last_block_query)

        if last_block is not None:
            return BlockInfo(
                hash=last_block['hash'],
                number=last_block['number'],
                timestamp=last_block['timestamp']
            )

    async def get_block_info(self, block_hash: str) -> Optional[BlockInfo]:
        query = get_block_number_by_hash_query(block_hash)
        async with self.pool.acquire() as conn:
            block = await fetch_row(conn, query=query)

        if block is not None:
            return BlockInfo(
                hash=block_hash,
                number=block['number']
            )

    async def get_blockchain_tip(self,
                                 tip_block: Optional[BlockInfo],
                                 last_block: Optional[BlockInfo] = None) -> Optional[BlockchainTip]:
        """
        Return status of client's last known block
        """
        last_block = last_block or await self.get_latest_block_info()

        is_in_fork = False
        last_unchanged = None
        if tip_block:
            split_query = select([chain_splits_t.c.common_block_number]).where(
                chain_splits_t.c.id == select([reorgs_t.c.split_id]).where(reorgs_t.c.block_hash == tip_block.hash)
            )

            async with self.pool.acquire() as conn:
                chain_split = await fetch_row(conn, query=split_query)

            is_in_fork = chain_split is not None
            common_block_number = chain_split and chain_split['common_block_number']
            if is_in_fork and common_block_number is not None:
                last_unchanged = common_block_number

        return BlockchainTip(
            tip_hash=tip_block and tip_block.hash,
            tip_number=tip_block and tip_block.number,
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
                                offset: int) -> List[Dict[str, Any]]:
        query = get_wallet_events_query(
            address=address,
            from_block=from_block,
            until_block=until_block,
            limit=limit,
            offset=offset,
            order=order
        )
        async with self.pool.acquire() as connection:
            events = await fetch(connection, query)

        events = in_app_distinct(events)

        result = OrderedDict()
        for event in events:
            tx_data = event['tx_data']
            if tx_data:
                tx = models.Transaction(**json.loads(tx_data)).to_dict()
            else:
                tx = {}

            tx_hash = event['tx_hash']
            tx_event = models.WalletEvent(**event).to_dict()

            item = result.get(tx_hash)
            if item:
                tx_events = item.get('events', [])
                tx_events.append(tx_event)

                item.update(events=tx_events)
            else:
                result[tx_hash] = {'rootTxData': tx, 'events': [tx_event]}

        return [value for key, value in result.items()]

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

    async def get_wallet_assets_summary(self,
                                        addresses: List[str],
                                        limit: int,
                                        offset: int,
                                        assets: Optional[List[str]] = None) -> AddressesSummary:
        query = get_assets_summary_query(addresses=addresses, assets=assets, limit=limit, offset=offset)

        async with self.pool.acquire() as conn:
            rows = await fetch(conn, query)

            addr_map = {k: list(g) for k, g in groupby(rows, lambda r: r['address'])}
            summary = []
            for address in addresses:
                nonce = 0
                assets_summary = []
                for row in addr_map.get(address, []):
                    value = row['value'] or "0"
                    decimals = row['decimals'] or "0"

                    balance = value and int(value)
                    decimals = decimals and int(decimals)

                    asset_summary = AssetSummary(
                        balance=str(balance),
                        decimals=str(decimals),
                        address=row['asset_address'],
                        transfers_number=row['tx_number'],
                    )
                    assets_summary.append(asset_summary)
                    if row['nonce']:
                        nonce = row['nonce']

                item = AddressSummary(
                    address=address,
                    assets_summary=assets_summary,
                    outgoing_transactions_number=str(nonce)
                )
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

    async def get_account_internal_transactions(
            self,
            account: str,
            limit: int,
            offset: int,
            order: str
    ) -> Tuple[List[models.InternalTransaction], Optional[LastAffectedBlock]]:

        query = get_internal_txs_by_account(account, order)
        query = query.limit(limit)
        query = query.offset(offset)

        rows = await fetch(self.pool, query)
        internal_txs = [models.InternalTransaction(**r) for r in rows]
        last_affected_block = max((r['block_number'] for r in rows), default=None)

        return internal_txs, last_affected_block

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
                'events': [models.WalletEvent(**event).to_dict()] if event else []
            }
            result.append(tx_data)

        return result
