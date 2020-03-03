import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass
from functools import partial
from itertools import groupby, chain
from typing import DefaultDict, Tuple, Set, NamedTuple, Union
from typing import List, Optional, Dict, Any

from aiopg.sa import Engine
from sqlalchemy import select, and_, desc, true

from jsearch.api import models
from jsearch.api.database_queries.account_bases import get_account_base_query
from jsearch.api.database_queries.account_states import get_account_state_query
from jsearch.api.database_queries.assets_summary import get_assets_summary_query
from jsearch.api.database_queries.blocks import (
    get_block_by_hash_query,
    get_block_by_number_query,
    get_block_number_by_hash_query,
    get_block_number_by_timestamp_query,
    get_blocks_by_number_query,
    get_blocks_by_timestamp_query,
    get_blocks_query,
    get_last_block_query,
    ORDER_SCHEME_BY_NUMBER,
    ORDER_SCHEME_BY_TIMESTAMP, generate_blocks_query)
from jsearch.api.database_queries.chain_events import (
    select_latest_chain_event_id,
    select_closest_chain_split,
)
from jsearch.api.database_queries.dex_logs import (
    get_dex_orders_query,
    get_dex_orders_events_query,
    get_dex_trades_query,
    get_dex_trades_events_query,
    get_dex_events_query,
    get_dex_blocked_query)
from jsearch.api.database_queries.internal_transactions import (
    get_internal_txs_by_parent,
    get_internal_txs_by_address_and_block_query,
    get_internal_txs_by_address_and_timestamp_query
)
from jsearch.api.database_queries.logs import (
    get_logs_by_address_and_block_query,
    get_logs_by_address_and_timestamp_query
)
from jsearch.api.database_queries.pending_transactions import (
    get_pending_txs_by_account,
    get_account_pending_txs_timestamp,
    get_outcoming_pending_txs_count,
    get_pending_txs_ordering
)
from jsearch.api.database_queries.token_holders import get_token_holders_query, get_last_token_holders_query
from jsearch.api.database_queries.token_transfers import (
    get_token_transfers_by_account_and_block_number,
    get_token_transfers_by_token_and_block_number
)
from jsearch.api.database_queries.transactions import (
    get_tx_by_hash,
    get_tx_by_address_and_block_query,
    get_tx_by_address_and_timestamp_query,
    get_transactions_by_hashes,
    get_block_txs_query,
)
from jsearch.api.database_queries.uncles import (
    get_uncles_by_timestamp_query,
    get_uncles_by_number_query,
    get_uncles_by_miner_address_and_timestamp_query,
    get_uncles_by_miner_address_and_number_query,
    get_uncles_query,
)
from jsearch.api.database_queries.wallet_events import (
    get_wallet_events_query,
    get_eth_transfers_by_address_query,
)
from jsearch.api.helpers import Tag, get_cursor_percent, TAG_LATEST
from jsearch.api.ordering import Ordering, ORDER_DESC, ORDER_SCHEME_NONE
from jsearch.api.structs import AddressesSummary, AssetSummary, AddressSummary, BlockchainTip, BlockInfo
from jsearch.api.structs.wallets import WalletEvent, WalletEventDirection
from jsearch.common.db import DbActionsMixin
from jsearch.common.processing.dex_logs import DexEventType, ORDER_STATUSES, ORDER_EVENT_TYPE_TO_STATUS
from jsearch.common.processing.wallet import ETHER_ASSET_ADDRESS
from jsearch.common.queries import in_app_distinct
from jsearch.common.tables import reorgs_t, chain_events_t, blocks_t
from jsearch.common.utils import unique
from jsearch.common.wallet_events import get_event_from_pending_tx
from jsearch.consts import NULL_ADDRESS
from jsearch.typing import LastAffectedBlock, OrderDirection, TokenAddress, ProgressPercent

logger = logging.getLogger(__name__)


def process_block(row: Dict[str, Any]) -> Dict[str, Any]:
    row.update({
        'uncles': row['uncles'] or [],
        'transactions': row['transactions'] or [],
        'static_reward': int(row['static_reward']),
        'uncle_inclusion_reward': int(row['uncle_inclusion_reward']),
        'tx_fees': int(row['tx_fees']),
    })
    return row


def _group_by_block(items: List[Dict[str, Any]]) -> DefaultDict[str, List[Dict[str, Any]]]:
    items_by_block: DefaultDict[str, List[Dict[str, Any]]] = defaultdict(list)
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


class Storage(DbActionsMixin):

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    async def get_latest_chain_event_id(self) -> Optional[int]:
        query = select_latest_chain_event_id()
        row = await self.fetch_one(query)
        return row['max_id'] if row else None

    async def is_data_affected_by_chain_split(
            self,
            last_known_chain_event_id: Optional[int],
            last_affected_block: Optional[int],
    ) -> bool:
        """
        --[15a]---[16a]---[17a]
               \
                \
                 -[16b]---[17b]

        Data is considered affected by chain split if after last memorized
        history state (i.e. last known chain event ID with type inserted)
        there's a chain split involving last figured block in any database
        request.
        """
        if last_known_chain_event_id is None or last_affected_block is None:
            return False

        query = select_closest_chain_split(
            last_known_chain_event_id=last_known_chain_event_id,
            last_affected_block=last_affected_block,
        )

        row = await self.fetch_one(query)
        return row is not None

    async def get_account(self, address, tag) -> Tuple[Optional[models.Account], Optional[LastAffectedBlock]]:
        account_state_query = get_account_state_query(address, tag)
        account_base_query = get_account_base_query(address)

        state_row = await self.fetch_one(account_state_query)

        if state_row is None:
            return None, None

        base_row = await self.fetch_one(account_base_query)

        state_row['balance'] = int(state_row['balance'])

        row = {**state_row, **base_row}  # type: ignore
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
        # Notes: syncer writes txs to main db with denormalization (x2 records per transaction)
        query_limit = limit * 2
        if ordering.scheme == ORDER_SCHEME_BY_NUMBER:
            query = get_tx_by_address_and_block_query(query_limit, address, block_number, ordering, tx_index)
        else:
            query = get_tx_by_address_and_timestamp_query(query_limit, address, timestamp, ordering, tx_index)

        rows = await self.fetch_all(query)
        rows = in_app_distinct(rows)[:limit]

        txs = [models.Transaction(**r) for r in rows]
        last_affected_block = max((r['block_number'] for r in rows), default=None)

        return txs, last_affected_block

    async def get_block_transactions(self, tag, tx_index=None):
        query = get_block_txs_query(tag, tx_index)

        rows = await self.fetch_all(query)

        # FAQ: `SELECT DISTINCT` performs two times slower than `SELECT`, so use
        # `in_app_distinct` instead.
        rows = in_app_distinct(rows)

        return [models.Transaction(**r) for r in rows]

    async def get_block_internal_transactions(self, tag, parent_tx_hash=None):
        fields = models.InternalTransaction.select_fields()
        params = [tag.value]

        if tag.is_hash():
            condition = "block_hash=%s AND is_forked=false"
        elif tag.is_number():
            condition = "block_number=%s AND is_forked=false"
        else:
            condition = "block_number=(SELECT max(number) FROM blocks) AND is_forked=false"
            params = []

        if parent_tx_hash:
            condition += " AND parent_tx_hash=%s"
            params.append(parent_tx_hash)

        q = f"""SELECT {fields} FROM internal_transactions
                    WHERE {condition} AND is_forked=false
                    ORDER BY transaction_index;"""

        if tag.is_latest():
            rows = await self.fetch_all(q)
        else:
            rows = await self.fetch_all(q, *params)
        return [models.InternalTransaction(**r) for r in rows]

    async def get_block(self, tag: Tag):
        if tag.is_hash():
            query = get_block_by_hash_query(block_hash=tag.value)

        elif tag.is_number():
            query = get_block_by_number_query(number=tag.value)

        else:
            query = get_last_block_query()

        row = await self.fetch_one(query)
        if row is None:
            return None

        row = process_block(row)
        return models.Block(**row)

    async def get_blocks(
            self,
            limit: int,
            order: Ordering,
            number: Optional[int] = None,
            timestamp: Optional[int] = None,
    ) -> Tuple[List[models.Block], ProgressPercent, Optional[LastAffectedBlock]]:
        get_query = partial(generate_blocks_query, number=number, timestamp=timestamp, limit=limit)

        query = get_query(order=order)
        reverse_query = get_query(order=order.reverse())

        rows = await self.fetch_all(query=query)
        rows = [process_block(row) for row in rows]

        progress = await get_cursor_percent(
            engine=self.engine,
            query=query,
            reverse_query=reverse_query
        )

        blocks = [models.Block(**row) for row in rows]
        last_affected_block = max((r['number'] for r in rows), default=None)

        return blocks, progress, last_affected_block

    async def get_account_mined_blocks(
            self,
            address: str,
            limit: int,
            order: Ordering,
            timestamp: Optional[int],
            number: Optional[int],
    ) -> Tuple[List[models.Block], Optional[LastAffectedBlock]]:
        if number is None and timestamp is None:
            query = get_blocks_query(limit=limit, order=order, miner=address)
        else:
            if order.scheme == ORDER_SCHEME_BY_TIMESTAMP:
                query = get_blocks_by_timestamp_query(  # type: ignore
                    limit=limit,
                    timestamp=timestamp,
                    order=order,
                    miner=address,
                )
            elif order.scheme == ORDER_SCHEME_BY_NUMBER:
                query = get_blocks_by_number_query(  # type: ignore
                    limit=limit,
                    number=number,
                    order=order,
                    miner=address,
                )
            else:
                raise ValueError('Invalid scheme: {scheme}')

        rows = await self.fetch_all(query)
        rows = [process_block(row) for row in rows]

        blocks = [models.Block(**row) for row in rows]
        last_affected_block = max((r['number'] for r in rows), default=None)

        return blocks, last_affected_block

    async def get_uncle(self, tag):
        if tag.is_hash():
            query = "SELECT * FROM uncles WHERE hash=%s AND is_forked=false"
        elif tag.is_number():
            query = "SELECT * FROM uncles WHERE number=%s AND is_forked=false"
        else:
            query = "SELECT * FROM uncles WHERE number=(SELECT max(number) FROM uncles) AND is_forked=false"

        if tag.is_latest():
            row = await self.fetch_one(query)
        else:
            row = await self.fetch_one(query, tag.value)

        if row is None:
            return None

        del row['block_hash']
        del row['is_forked']
        row['reward'] = int(row['reward'])
        return models.Uncle(**row)

    async def get_uncles(
            self,
            limit: int,
            order: Ordering,
            number: Optional[int] = None,
            timestamp: Optional[int] = None
    ) -> Tuple[List[models.Uncle], Optional[LastAffectedBlock]]:
        if number is None and timestamp is None:
            query = get_uncles_query(limit=limit, order=order)
        else:
            if order.scheme == ORDER_SCHEME_BY_TIMESTAMP:
                query = get_uncles_by_timestamp_query(limit=limit, timestamp=timestamp, order=order)  # type: ignore

            elif order.scheme == ORDER_SCHEME_BY_NUMBER:
                query = get_uncles_by_number_query(limit, number=number, order=order)  # type: ignore

            else:
                raise ValueError('Invalid scheme: {scheme}')

        rows = await self.fetch_all(query=query)
        for row in rows:
            row.update({
                'reward': int(row['reward']),
            })

        uncles = [models.Uncle(**row) for row in rows]
        last_affected_block = max((r['block_number'] for r in rows), default=None)

        return uncles, last_affected_block

    async def get_account_mined_uncles(
            self,
            address: str,
            limit: int,
            order: Ordering,
            number: Optional[int] = None,
            timestamp: Optional[int] = None
    ) -> Tuple[List[models.Uncle], Optional[LastAffectedBlock]]:
        if number is None and timestamp is None:
            query = get_uncles_query(limit=limit, order=order, address=address)
        else:
            if order.scheme == ORDER_SCHEME_BY_TIMESTAMP:
                query = get_uncles_by_miner_address_and_timestamp_query(  # type: ignore
                    address=address,
                    limit=limit,
                    timestamp=timestamp,
                    order=order
                )

            elif order.scheme == ORDER_SCHEME_BY_NUMBER:
                query = get_uncles_by_miner_address_and_number_query(  # type: ignore
                    address=address,
                    limit=limit,
                    number=number,
                    order=order
                )

            else:
                raise ValueError('Invalid scheme: {scheme}')

        rows = await self.fetch_all(query=query)
        for row in rows:
            row.update({
                'reward': int(row['reward']),
            })

        uncles = [models.Uncle(**row) for row in rows]
        last_affected_block = max((r['block_number'] for r in rows), default=None)

        return uncles, last_affected_block

    async def get_block_uncles(self, tag, uncle_index=None):
        if tag.is_hash():
            query = "SELECT * FROM uncles WHERE block_hash=%s AND is_forked=false"
        elif tag.is_number():
            query = "SELECT * FROM uncles WHERE block_number=%s AND is_forked=false"
        else:
            query = "SELECT * FROM uncles WHERE block_number=(SELECT max(number) FROM blocks) AND is_forked=false"

        if tag.is_latest():
            rows = await self.fetch_all(query)
        else:
            rows = await self.fetch_all(query, tag.value)

        for r in rows:
            del r['block_hash']
            del r['is_forked']
            r['reward'] = int(r['reward'])

        if uncle_index is not None:
            if uncle_index < len(rows):
                return [models.Uncle(**rows[uncle_index])]
            else:
                return []
        else:
            return [models.Uncle(**r) for r in rows]

    async def get_transaction(self, tx_hash):
        query = get_tx_by_hash(tx_hash)
        row = await self.fetch_one(query)
        if row:
            return models.Transaction(**row)

    async def get_receipt(self, tx_hash):
        query = "SELECT * FROM receipts WHERE transaction_hash=%s AND is_forked=false"
        row = await self.fetch_one(query, tx_hash)
        if row is None:
            return None
        row = dict(row)
        del row['is_forked']
        row['logs'] = await self.get_logs(row['transaction_hash'])
        return models.Receipt(**row)

    async def get_logs(self, tx_hash: str) -> List[models.Log]:
        fields = models.Log.select_fields()
        query = f"SELECT {fields} FROM logs WHERE transaction_hash=%s AND is_forked=false ORDER BY log_index"

        rows = await self.fetch_all(query, tx_hash)
        return [models.Log(**r) for r in rows]

    async def get_account_logs(
            self,
            address: str,
            limit: int,
            ordering: Ordering,
            block_number: Optional[int],
            timestamp: Optional[int],
            transaction_index: Optional[int],
            log_index: Optional[int],
    ) -> Tuple[List[models.Log], Optional[LastAffectedBlock]]:
        if ordering.scheme == ORDER_SCHEME_BY_NUMBER:
            query = get_logs_by_address_and_block_query(
                address=address,
                limit=limit,
                ordering=ordering,
                block_number=block_number,
                transaction_index=transaction_index,
                log_index=log_index,
            )
        else:
            query = get_logs_by_address_and_timestamp_query(
                address=address,
                limit=limit,
                ordering=ordering,
                timestamp=timestamp,
                transaction_index=transaction_index,
                log_index=log_index,
            )

        rows = await self.fetch_all(query)

        logs = [models.Log(**r) for r in rows]
        last_affected_block = max((r['block_number'] for r in rows), default=None)
        return logs, last_affected_block

    async def get_accounts_balances(self, addresses) -> Tuple[List[models.Balance], Optional[LastAffectedBlock]]:
        queries = list()

        for address in addresses:
            queries.append(get_account_state_query(address, TAG_LATEST))

        coros = [self.fetch_one(query) for query in queries]
        rows = await asyncio.gather(*coros)
        rows = [r for r in rows if r is not None]

        addr_map = {r['address']: r for r in rows}

        balances = []
        for address in addresses:
            if address in addr_map:
                balance = models.Balance(
                    balance=int(addr_map[address]['balance']),
                    address=addr_map[address]['address']
                )
                balances.append(balance)
        last_affected_block = max((r['block_number'] for r in rows), default=None)
        return balances, last_affected_block

    async def get_tokens_transfers(
            self,
            address: str,
            limit: int,
            ordering: Ordering,
            block_number: int,
            account_address: Optional[str] = None,
            transaction_index: Optional[int] = None,
            log_index: Optional[int] = None
    ) -> Tuple[List[models.TokenTransfer], Optional[LastAffectedBlock]]:
        # HACK: There're 2 times more entries due to denormalization, see
        # `log_to_transfers`. Because of this, `offset` and `limit` should be
        # multiplied first and rows should be deduped second.
        query_limit = limit * 2

        query = get_token_transfers_by_token_and_block_number(
            address=address,
            ordering=ordering,
            limit=query_limit,
            block_number=block_number,
            account_address=account_address,
            transaction_index=transaction_index,
            log_index=log_index
        )

        rows = await self.fetch_all(query)
        # FAQ: `SELECT DISTINCT` performs two times slower than `SELECT`, so use
        # `in_app_distinct` instead.
        rows_distinct = in_app_distinct(rows)[:limit]

        transfers = [models.TokenTransfer(**value) for value in rows_distinct]
        last_affected_block = max((r['block_number'] for r in rows), default=None)

        return transfers, last_affected_block

    async def get_account_tokens_transfers(
            self,
            address: str,
            limit: int,
            ordering: Ordering,
            block_number: int,
            transaction_index: Optional[int] = None,
            log_index: Optional[int] = None
    ) -> Tuple[List[models.TokenTransfer], Optional[LastAffectedBlock]]:

        # HACK: There're 2 times more entries due to denormalization, see
        # `log_to_transfers`. Because of this, `offset` and `limit` should be
        # multiplied first and rows should be deduped second.
        query_limit = limit * 2

        query = get_token_transfers_by_account_and_block_number(
            address=address,
            ordering=ordering,
            limit=query_limit,
            block_number=block_number,
            transaction_index=transaction_index,
            log_index=log_index
        )

        rows = await self.fetch_all(query)
        # FAQ: `SELECT DISTINCT` performs two times slower than `SELECT`, so use
        # `in_app_distinct` instead.
        rows_distinct = in_app_distinct(rows)[:limit]

        transfers = [models.TokenTransfer(**value) for value in rows_distinct]
        last_affected_block = max((r['block_number'] for r in rows), default=None)

        return transfers, last_affected_block

    async def get_tokens_holders(
            self,
            limit: int,
            ordering: Ordering,
            token_address: TokenAddress,
            balance: Optional[int],
            _id: Optional[int] = None
    ) -> Tuple[List[models.TokenHolderWithId], Optional[LastAffectedBlock]]:
        query = get_token_holders_query(
            limit=limit,
            ordering=ordering,
            token_address=token_address,
            balance=balance,
            _id=_id
        )
        rows = await self.fetch_all(query)

        holders = [models.TokenHolderWithId(**r) for r in rows]
        last_affected_block = max((r['block_number'] for r in rows), default=None)

        return holders, last_affected_block

    async def get_account_token_balance(
            self,
            account_address: str,
            token_address: str
    ) -> Tuple[Optional[models.TokenHolder], Optional[LastAffectedBlock]]:
        query = get_last_token_holders_query(
            account_address=account_address,
            token_addresses=[token_address]
        )

        row = await self.fetch_one(query)
        if not row:
            return None, None

        holder = models.TokenHolder(**row)
        last_affected_block = row['block_number']

        return holder, last_affected_block

    async def get_account_tokens_balances(
            self,
            account_address: str,
            tokens_addresses: List[str]
    ) -> Tuple[List[models.TokenBalance], Optional[LastAffectedBlock]]:
        if not tokens_addresses:
            return [], None

        query = get_last_token_holders_query(
            account_address=account_address,
            token_addresses=tokens_addresses
        )

        rows = await self.fetch_all(query)

        last_affected_block = max([r['block_number'] for r in rows], default=None)
        balances = [models.TokenBalance(**row) for row in rows]

        return balances, last_affected_block

    async def get_latest_block_info(self) -> Optional[BlockInfo]:
        last_block_query = get_last_block_query()
        last_block = await self.fetch_one(query=last_block_query)

        if last_block is not None:
            return BlockInfo(
                hash=last_block['hash'],
                number=last_block['number'],
                timestamp=last_block['timestamp']
            )

        return None

    async def get_block_info(self, block_hash: str) -> Optional[BlockInfo]:
        query = get_block_number_by_hash_query(block_hash)
        block = await self.fetch_one(query=query)

        if block is not None:
            return BlockInfo(
                hash=block_hash,
                number=block['number'],
                timestamp=block['timestamp'],
                is_forked=block['is_forked']
            )

        return None

    async def get_block_by_timestamp(self, timestamp: int, order_direction: OrderDirection) -> Optional[BlockInfo]:
        query = get_block_number_by_timestamp_query(timestamp, order_direction)
        block = await self.fetch_one(query=query)

        if block is not None:
            return BlockInfo(
                hash=block['hash'],
                number=block['number'],
                timestamp=block['timestamp']
            )

        return None

    async def get_blockchain_tip(self,
                                 tip_block: Optional[BlockInfo],
                                 last_block: Optional[BlockInfo] = None) -> BlockchainTip:
        """
        Return status of client's last known block
        """
        last_block = last_block or await self.get_latest_block_info()

        is_in_fork = False
        last_unchanged = None

        get_not_forked_block_query = select(
            [
                blocks_t.c.hash,
            ]
        ).where(
            and_(
                # FIXME (nickgashkov): `tip_block` could be `None`.
                blocks_t.c.hash == tip_block.hash,  # type: ignore
                blocks_t.c.is_forked == true()
            )
        ).order_by(
            desc(blocks_t.c.number)
        ).limit(1)

        get_chain_events_id_query = select(
            [
                reorgs_t.c.split_id
            ]
        ).where(
            reorgs_t.c.block_hash == get_not_forked_block_query
        ).order_by(
            desc(reorgs_t.c.split_id)
        ).limit(1)

        if tip_block:
            split_query = select(
                [
                    chain_events_t.c.block_number
                ]
            ).where(
                chain_events_t.c.id == get_chain_events_id_query
            ).order_by(chain_events_t.c.block_number)

            chain_split = await self.fetch_one(query=split_query)

            is_in_fork = tip_block.is_forked or chain_split is not None
            if is_in_fork:

                # Notes:
                # If we don't have reorg record and tip block is in fork state
                # we return previous block before tip as last unchanged
                if chain_split:
                    last_unchanged = chain_split and chain_split['block_number']
                else:
                    # FIXME (nickgashkov): `tip_block` could be `None`.
                    last_unchanged = tip_block.number - 1 if tip_block.number > 1 else 0  # type: ignore

        return BlockchainTip(  # type: ignore
            tip_hash=tip_block and tip_block.hash,
            tip_number=tip_block and tip_block.number,
            # FIXME (nickgashkov): `last_block` could be `None`.
            last_hash=last_block.hash,  # type: ignore
            last_number=last_block.number,  # type: ignore
            is_in_fork=is_in_fork,
            last_unchanged_block=last_unchanged
        )

    async def get_wallet_events(
            self,
            address: str,
            block_number: int,
            limit: int,
            tx_index: Optional[int],
            event_index: int,
            ordering: Ordering
    ) -> Tuple[List[WalletEvent], Optional[ProgressPercent], Optional[LastAffectedBlock]]:
        # Notes: syncer writes txs to main db with denormalization (x2 records per transaction)
        query_limit = limit * 2

        get_query = partial(
            get_wallet_events_query,
            limit=query_limit,
            address=address,
            block_number=block_number,
            tx_index=tx_index,
            event_index=event_index,
        )

        query = get_query(ordering=ordering)
        reverse_query = get_query(ordering=ordering.reverse())
        events = await self.fetch_all(query)
        progress = await get_cursor_percent(
            engine=self.engine,
            query=query,
            reverse_query=reverse_query
        )

        events = in_app_distinct(events)[:limit]

        tx_hashes = {e['tx_hash'] for e in events}
        tx_query = get_transactions_by_hashes(tx_hashes)
        transactions = await self.fetch_all(tx_query)
        transactions_map = {tx['hash']: tx for tx in transactions}

        wallet_events = []
        for event in events:
            tx_data = transactions_map.get(event['tx_hash'])
            tx_data['value'] = str(int(tx_data['value'], 16))  # type: ignore
            if tx_data:
                tx = models.Transaction(**tx_data).to_dict()
            else:
                tx = {}
            event_data = event['event_data']
            direction = WalletEventDirection.IN if event_data['recipient'] == address else WalletEventDirection.OUT
            # FIXME (nickgashkov): Consider using `__getitem__` or handle `None` values.
            wallet_event = WalletEvent(  # type: ignore
                type=event.get('type'),
                event_index=event.get('event_index'),
                event_data=event.get('event_data'),
                transaction=tx,
                direction=direction
            )
            wallet_events.append(wallet_event)

        last_affected_block = max([event['blockNumber'] for event in wallet_events], default=None)  # type: ignore
        return wallet_events, progress, last_affected_block

    async def get_wallet_assets_summary(
            self,
            addresses: List[str],
            assets: Optional[List[str]] = None
    ) -> Tuple[AddressesSummary, LastAffectedBlock]:
        # See: https://jibrelnetwork.atlassian.net/browse/ETHBE-801
        addresses_contains_null_address = NULL_ADDRESS in addresses
        addresses = [a for a in addresses if a != NULL_ADDRESS]

        query = get_assets_summary_query(addresses=unique(addresses), assets=unique(assets or []))
        rows = await self.fetch_all(query)

        account_balances: DefaultDict[str, List[Dict[str, Any]]] = defaultdict(list)
        accounts_with_ether: Set[str] = set()

        for asset in rows:
            account_balances[asset['address']].append(asset)

            if asset['asset_address'] == ETHER_ASSET_ADDRESS:
                accounts_with_ether.add(asset['address'])

        summary = []

        if addresses_contains_null_address:
            # Return fake Ether balance for Null Address account. This is done
            # for lowering the load on the database but keep API the same.
            #
            # SEE: https://jibrelnetwork.atlassian.net/browse/ETHBE-801
            summary.append(
                AddressSummary(
                    address=NULL_ADDRESS,
                    assets_summary=[
                        AssetSummary(
                            balance="0",
                            decimals="0",
                            address=ETHER_ASSET_ADDRESS,
                            transfers_number=0,
                        )
                    ],
                    outgoing_transactions_number="0",
                )
            )

        for account in addresses:
            nonce = 0
            account_summary: List[AssetSummary] = []
            for asset in account_balances[account]:
                if asset['nonce']:
                    nonce = asset['nonce']

                value = asset['value'] or "0"
                decimals = asset['decimals'] or "0"

                balance = value and int(value)
                decimals = decimals and int(decimals)

                asset_summary = AssetSummary(
                    balance=str(balance),
                    decimals=str(decimals),
                    address=asset['asset_address'],
                    transfers_number=0,
                )
                account_summary.append(asset_summary)

            if account not in accounts_with_ether:
                # Return fake Ether balance for an account even if there's no
                # such summary for an account. This allows simplifying
                # client-side logic.
                #
                # This can happen if account has been created, but never mined a
                # blocks and received/sent Ether.
                account_summary.append(
                    AssetSummary(
                        balance="0",
                        decimals="0",
                        address=ETHER_ASSET_ADDRESS,
                        transfers_number=0,
                    ),
                )

            item = AddressSummary(
                address=account,
                assets_summary=sorted(account_summary, key=lambda x: x.address),
                outgoing_transactions_number=str(nonce)
            )
            if item.assets_summary:
                summary.append(item)

        last_affected_block_number = max([r['block_number'] or 0 for r in rows], default=None)
        return summary, last_affected_block_number

    async def get_nonce(self, address):
        """
        Get account nonce
        """
        query = """
            SELECT "nonce" FROM accounts_state
            WHERE address=%s AND is_forked=false ORDER BY block_number DESC LIMIT 1;
        """
        row = await self.fetch_one(query, address)
        if row:
            return row['nonce']
        return 0

    async def get_internal_transactions(self, parent_tx_hash: str, order: str):
        query = get_internal_txs_by_parent(parent_tx_hash, order)
        rows = await self.fetch_all(query)
        internal_txs = [models.InternalTransaction(**r) for r in rows]
        return internal_txs

    async def get_account_internal_transactions(
            self,
            address: str,
            limit: int,
            ordering: Ordering,
            block_number: int,
            timestamp: int,
            parent_tx_index: Optional[int] = None,
            tx_index: Optional[int] = None
    ) -> Tuple[List[models.InternalTransaction], Optional[LastAffectedBlock]]:

        if ordering.scheme == ORDER_SCHEME_BY_NUMBER:
            query = get_internal_txs_by_address_and_block_query(
                limit=limit,
                address=address,
                block_number=block_number,
                ordering=ordering,
                tx_index=tx_index,
                parent_tx_index=parent_tx_index,
            )
        else:
            query = get_internal_txs_by_address_and_timestamp_query(
                limit=limit,
                address=address,
                timestamp=timestamp,
                ordering=ordering,
                tx_index=tx_index,
                parent_tx_index=parent_tx_index,
            )

        rows = await self.fetch_all(query)

        txs = [models.InternalTransaction(**r) for r in rows]
        last_affected_block = max((r['block_number'] for r in rows), default=None)

        return txs, last_affected_block

    async def get_account_pending_transactions(
            self,
            account: str, limit: int,
            ordering: Ordering,
            timestamp: int,
            id: Optional[int],
    ) -> List[models.PendingTransaction]:
        query = get_pending_txs_by_account(account, limit, ordering, timestamp, id)
        rows = await self.fetch_all(query)
        for row in rows:
            row['timestamp'] = row['timestamp'] and int(row['timestamp'].timestamp())
        return [models.PendingTransaction(**r) for r in rows]

    async def get_account_pending_tx_timestamp(
            self,
            account: str,
            ordering: Ordering,
    ) -> Optional[int]:
        query = get_account_pending_txs_timestamp(account, ordering)
        row = await self.fetch_one(query)
        if row:
            value = row['timestamp']
            return value and value

        return None

    async def get_account_pending_events(self, account: str, limit: int) -> List[Dict[str, Any]]:
        ordering = get_pending_txs_ordering(scheme=ORDER_SCHEME_NONE, direction=ORDER_DESC)
        query = get_pending_txs_by_account(account, limit, ordering, )

        if limit:
            query = query.limit(limit)

        rows = await self.fetch_all(query)

        result = []
        for tx in rows:
            event = get_event_from_pending_tx(address=account, pending_tx=tx)
            if event:
                event_data = event['event_data']
                direction = WalletEventDirection.IN if event_data['recipient'] == account else WalletEventDirection.OUT
                # FIXME (nickgashkov): Consider using `__getitem__` or handle `None` values.
                event = WalletEvent(  # type: ignore
                    type=event.get('type'),
                    event_index=event.get('event_index'),
                    event_data=event.get('event_data'),
                    direction=direction,
                    transaction=models.PendingTransaction(**tx).to_dict()
                )
            tx_data = {
                'transaction': models.PendingTransaction(**tx).to_dict(),
                'events': [event.to_dict()] if event is not None else None  # type: ignore
            }
            result.append(tx_data)

        return result

    async def get_account_transaction_count(self, account_address, include_pending_txs=True):
        res = await self.get_nonce(account_address)
        if include_pending_txs:
            query = get_outcoming_pending_txs_count(account_address)
            rows = await self.fetch_all(query)
            res += rows[0]['count_1']
        return res

    async def get_account_eth_transfers(self, account_address, block_number=None,
                                        event_index=None, order='desc', limit=20):
        query = get_eth_transfers_by_address_query(address=account_address, block_number=block_number,
                                                   event_index=event_index,
                                                   ordering=order, limit=limit)
        rows = await self.fetch_all(query)
        last_affected_block_number = max([r['block_number'] for r in rows], default=None)

        tx_hashes = {r['tx_hash'] for r in rows}
        tx_query = get_transactions_by_hashes(tx_hashes)
        transactions = await self.fetch_all(tx_query)
        transactions_map = {tx['hash']: tx for tx in transactions}

        res = []
        for row in rows:
            event_data = row['event_data']
            tx_data = transactions_map[row['tx_hash']]
            t = models.EthTransfer(**{
                # NOTE: As of now, older wallet events have no
                # `tx_data['timestamp']` because it was added after the start of
                # the sync (See #288).
                'timestamp': tx_data.get('timestamp'),
                'tx_hash': row['tx_hash'],
                'amount': event_data['amount'],
                'from': event_data['sender'],
                'to': event_data['recipient'],
                'block_number': row['block_number'],
                'event_index': row['event_index'],
            })
            res.append(t)

        return res, last_affected_block_number

    async def get_asset_placed_order(
            self,
            token_address: str,
    ) -> Optional[Dict[str, Any]]:
        orders_query = get_dex_orders_query(traded_asset=token_address).limit(1)
        result = await self.fetch_one(orders_query)
        if result:
            return result
        return None

    async def get_dex_history(
            self,
            ordering: Ordering,
            limit: int,
            token_address: str,
            event_type: Optional[List[str]] = None,
            block_number: Optional[int] = None,
            timestamp: Optional[int] = None,
            event_index: Optional[int] = None,
    ) -> Tuple[List['HistoryEvent'], Optional[int]]:
        # WTF: only orders have information about assets
        orders_query = get_dex_orders_query(traded_asset=token_address)
        orders = await self.fetch_all(orders_query)
        orders_map = {get_order_id(item): item for item in orders}
        orders_ids = list(orders_map.keys())

        trades_query = get_dex_trades_query(order_ids=orders_ids)
        trades = await self.fetch_all(trades_query)
        trades_map = {get_trade_id(item): item for item in trades}
        trades_ids = list(trades_map.keys())

        trades_orders_map = {get_trade_id(item): get_order_id(item) for item in trades}

        history_query = get_dex_events_query(
            limit=limit,
            orders_ids=orders_ids,
            trades_ids=trades_ids,
            ordering=ordering,
            block_number=block_number,
            timestamp=timestamp,
            event_index=event_index,
            events_types=event_type
        )
        history = await self.fetch_all(history_query)

        events = sorted(history, key=lambda x: x['timestamp'])
        last_affected_block = events[-1]['block_number'] if events else None

        result = []
        for event in history:
            description = EventDescription(
                event_type=event['event_type'],
                event_block_number=event['block_number'],
                event_timestamp=event['timestamp'],
                event_index=event['event_index']
            )

            if description.event_type in DexEventType.TRADE:
                trade_id = get_trade_id(event)
                order_id = trades_orders_map[trade_id]
            else:
                order_id = get_order_id(event)
                trade_id = None

            order_data = orders_map[order_id]
            order_payload = order_data['event_data']
            order_event = OrderEvent(
                order_creator=order_payload['orderCreator'],
                order_creation_timestamp=order_data['timestamp'],
                order_id=order_id,
                order_type=order_payload['orderType'],
                traded_asset=order_payload['tradedAsset'],
                traded_asset_amount=str(order_payload['tradedAmount']),
                fiat_asset=order_payload['fiatAsset'],
                fiat_price=str(order_payload['assetPrice']),
                expiration_timestamp=order_payload['expirationTimestamp'],
            )

            trade_event = None
            if trade_id:
                trade_data = trades_map[trade_id]
                trade_payload = trade_data['event_data']
                trade_event = TradeEvent(
                    trade_id=str(trade_id),
                    trade_creator=trade_payload['tradeCreator'],
                    trade_creation_timestamp=trade_data['timestamp'],
                    trade_amount=str(trade_payload['tradedAmount'])
                )

            history_event = HistoryEvent(description, order_event, trade_event)
            result.append(history_event)

        return result, last_affected_block

    async def get_dex_orders(
            self,
            token_address: str,
            order_creator: Optional[str] = None,
            order_statuses: Optional[List[str]] = None,
    ):
        # WTF: only orders have information about traded assets
        orders_query = get_dex_orders_query(creator=order_creator, traded_asset=token_address)
        orders = await self.fetch_all(orders_query)

        orders_map = {get_order_id(item): item for item in orders}
        orders_ids = list(orders_map.keys())

        order_events_query = get_dex_orders_events_query(orders_ids)
        order_events = await self.fetch_all(order_events_query)
        order_states = get_last_order_states(orders, order_events)

        trades_query = get_dex_trades_query(order_ids=orders_ids)
        trades = await self.fetch_all(trades_query)

        trades_map = {get_trade_id(item): item for item in trades}
        trades_ids = list(trades_map.keys())

        trades_events_query = get_dex_trades_events_query(trade_ids=trades_ids)
        trades_events = await self.fetch_all(trades_events_query)
        trades_states = get_last_trade_states(trades, trades_events)

        if order_statuses:
            event_types = [ORDER_STATUSES[status] for status in order_statuses]
            orders = [order for order in orders if order_states[get_order_id(order)]['event_type'] in event_types]

        completed_trades = []
        for trade in trades:
            trade_id = get_trade_id(trade)
            state = trades_states[trade_id]
            if state['event_type'] == DexEventType.TRADE_COMPLETED:
                completed_trades.append(trade)

        sorted_trades = sorted(completed_trades, key=get_order_id)
        trades_by_orders = groupby(sorted_trades, key=get_order_id)
        trades_sum_by_orders = {order_id: sum(map(get_traded_amount, trades)) for order_id, trades in trades_by_orders}

        results = []
        for order in orders:
            payload = order['event_data']
            order_id = payload['orderID']
            data = OrderDataInfo(
                order_creator=payload['orderCreator'],
                order_creation_timestamp=order['timestamp'],
                order_id=order_id,
                order_type=payload['orderType'],
                traded_asset=payload['tradedAsset'],
                traded_asset_amount=str(payload['tradedAmount']),
                fiat_asset=payload['fiatAsset'],
                fiat_price=str(payload['assetPrice']),
                expiration_timestamp=payload['expirationTimestamp'],
            )

            state = order_states[order_id]
            trades_sump = trades_sum_by_orders.get(order_id, 0)

            order_status = ORDER_EVENT_TYPE_TO_STATUS[state['event_type']]
            status = OrderStatusInfo(
                order_status=order_status,
                order_status_block_number=state['block_number'],
                remaining_traded_asset_amount=str(get_traded_amount(order) - trades_sump)
            )
            results.append(OrderInfo(order_data=data, order_status=status))

        all_events = chain(orders, order_events, trades, trades_events)
        sorted_events = sorted(all_events, key=lambda x: x['block_number'])
        last_affected_block = sorted_events[-1]['block_number'] if sorted_events else None
        return results, last_affected_block

    async def get_dex_blocked(
            self,
            user_address: str,
            token_addresses: Optional[List[str]],
    ):
        events_query = get_dex_blocked_query(user_address, token_addresses)
        events = await self.fetch_all(events_query)

        events = sorted(events, key=get_asset_address)

        total: DefaultDict[str, int] = defaultdict(lambda: 0)
        for asset, blocked_amounts in groupby(events, key=get_asset_address):
            for event in blocked_amounts:
                value = event['event_data']['assetAmount']

                if event['event_type'] == DexEventType.TOKEN_UNBLOCKED:
                    value *= -1

                total[asset] += value

        blocked_amounts = {BlockedAssetAmount(asset, str(value)) for asset, value in total.items()}

        sorted_events = sorted(events, key=lambda x: x['block_number'])
        last_affected_block = sorted_events[-1]['block_number'] if sorted_events else None
        return blocked_amounts, last_affected_block


def get_event_value(event: Dict[str, Any], key: str) -> Any:
    return event['event_data'][key]


get_trade_id = partial(get_event_value, key='tradeID')
get_order_id = partial(get_event_value, key='orderID')
get_traded_amount = partial(get_event_value, key='tradedAmount')
get_asset_address = partial(get_event_value, key='assetAddress')


def get_last_states(
        orders: List[Dict[str, Any]],
        status_change_events: List[Dict[str, Any]],
        id_key: str
) -> Dict[str, Any]:
    states = {}
    events = sorted(chain(orders, status_change_events), key=lambda x: (get_event_value(x, id_key), x['timestamp']))
    for order_id, order_states in groupby(events, key=lambda x: get_event_value(x, id_key)):
        last_state: Dict[str, Any] = list(order_states)[-1]
        states[order_id] = last_state
    return states


get_last_order_states = partial(get_last_states, id_key='orderID')
get_last_trade_states = partial(get_last_states, id_key='tradeID')


class OrderStatusInfo(NamedTuple):
    order_status: str
    order_status_block_number: int
    remaining_traded_asset_amount: str


class OrderDataInfo(NamedTuple):
    order_creator: str
    order_creation_timestamp: int
    order_id: int
    order_type: str
    traded_asset: str
    traded_asset_amount: str
    fiat_asset: str
    fiat_price: str
    expiration_timestamp: int


class OrderInfo(NamedTuple):
    order_data: OrderDataInfo
    order_status: OrderStatusInfo

    def as_dict(self):
        return {
            'order_data': self.order_data._asdict(),
            'order_status': self.order_status._asdict(),
        }


class OrderEvent(NamedTuple):
    order_id: int
    order_creator: str
    order_creation_timestamp: int
    order_type: str
    traded_asset: str
    traded_asset_amount: str
    fiat_asset: str
    fiat_price: str
    expiration_timestamp: int


class TradeEvent(NamedTuple):
    trade_id: str
    trade_amount: str
    trade_creator: str
    trade_creation_timestamp: int


class EventDescription(NamedTuple):
    event_type: str
    event_block_number: int
    event_timestamp: int
    event_index: int


@dataclass
class HistoryEvent:
    event_data: EventDescription
    order_data: OrderEvent
    trade_data: Optional[TradeEvent] = None

    def __getitem__(self, item) -> Optional[Union[str, int]]:
        if item == 'block_number':
            return str(self.event_data.event_block_number)

        if item == 'timestamp':
            return self.event_data.event_timestamp

        if item == 'event_index':
            return self.event_data.event_index

        return None

    def as_dict(self):
        data = {
            'event_data': self.event_data._asdict(),
            'order_data': self.order_data._asdict()
        }
        if self.trade_data:
            data['trade_data'] = self.trade_data._asdict()

        return data


class BlockedAssetAmount(NamedTuple):
    asset_address: str
    blocked_amount: str
