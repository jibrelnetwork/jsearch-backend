from sqlalchemy import select, Column, and_, tuple_
from sqlalchemy.orm import Query
from typing import List, Optional

from jsearch.api.helpers import get_order
from jsearch.common.tables import wallet_events_t
from jsearch.common.wallet_events import WalletEventType


def get_default_fields() -> List[Column]:
    return [
        wallet_events_t.c.type,
        wallet_events_t.c.event_index,
        wallet_events_t.c.event_data,
        wallet_events_t.c.tx_hash,
        wallet_events_t.c.tx_data,
    ]


def get_wallet_events_query(address: str,
                            from_block: int,
                            until_block: int,
                            limit: int,
                            offset: int,
                            order: str,
                            columns: Optional[List[Column]] = None) -> Query:
    columns = columns or get_default_fields()
    query = select(columns).where(
        and_(
            wallet_events_t.c.address == address,
        )
    )
    ordering = get_order(
        columns=[
            wallet_events_t.c.block_number,
            wallet_events_t.c.event_index,
        ],
        direction=order
    )
    query = query.order_by(*ordering)

    if from_block > until_block:
        from_block, until_block = until_block, from_block

    query = query.where(wallet_events_t.c.block_number.between(from_block, until_block))
    query = query.limit(limit)
    if offset:
        query = query.offset(offset)

    return query


def get_eth_transfers_by_address_query(address: str, block_number=None,
                                       event_index=None, timestamp=None, order='desc'):

    page_filter_fields = []
    page_filter_values = []

    if block_number:
        page_filter_fields.append(wallet_events_t.c.block_number)
        page_filter_values.append(int(block_number))
    elif timestamp:
        page_filter_fields.append(wallet_events_t.c.timestamp)
        page_filter_values.append(int(timestamp))

    if event_index:
        page_filter_fields.append(wallet_events_t.c.event_index)
        page_filter_values.append(int(event_index))

    filter_criteria = [
        wallet_events_t.c.is_forked == False,
        wallet_events_t.c.address == address,
    ]

    if page_filter_fields:
        filter_criteria.append(tuple_(*page_filter_fields) >= tuple_(*page_filter_values))

    filter_criteria.append(wallet_events_t.c.type == WalletEventType.ETH_TRANSFER)

    if order == 'desc':
        order_criteria = wallet_events_t.c.event_index.desc()
    elif order == 'asc':
        order_criteria = wallet_events_t.c.event_index.asc()
    else:
        raise ValueError(f'Invalid order value {order}')

    filter_criteria.append(wallet_events_t.c.type == WalletEventType.ETH_TRANSFER)
    query = wallet_events_t.select().where(
        and_(*filter_criteria)
    ).order_by(order_criteria)
    return query


