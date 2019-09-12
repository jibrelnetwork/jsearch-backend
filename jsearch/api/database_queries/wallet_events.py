import functools

from sqlalchemy import select, Column, and_, false
from sqlalchemy.orm import Query
from typing import List, Optional

from jsearch.api.ordering import Ordering, ORDER_SCHEME_BY_NUMBER, ORDER_SCHEME_BY_TIMESTAMP, get_ordering, ORDER_DESC
from jsearch.common.tables import wallet_events_t
from jsearch.common.wallet_events import make_event_index, WalletEventType
from jsearch.typing import OrderScheme, OrderDirection, Columns


def get_default_fields() -> List[Column]:
    return [
        wallet_events_t.c.block_number,
        wallet_events_t.c.type,
        wallet_events_t.c.event_index,
        wallet_events_t.c.event_data,
        wallet_events_t.c.tx_hash,
        wallet_events_t.c.tx_data,
    ]


def get_events_ordering(scheme: OrderScheme, direction: OrderDirection) -> Ordering:
    columns: Columns = {
        ORDER_SCHEME_BY_NUMBER: [wallet_events_t.c.event_index],
        ORDER_SCHEME_BY_TIMESTAMP: [wallet_events_t.c.event_index],
    }[scheme]
    return get_ordering(columns, scheme, direction)


def get_wallet_events_query(
        limit: int,
        address: str,
        ordering: Ordering,
        block_number: int,
        type_: Optional[str] = None,
        tx_index: Optional[int] = None,
        event_index: Optional[int] = None,
) -> Query:
    columns = get_default_fields()
    query = select(columns).where(
        and_(
            wallet_events_t.c.is_forked == false(),
            wallet_events_t.c.address == address,
        )
    )

    if type_ is not None:
        query = query.where(wallet_events_t.c.type == type_)

    if ordering.direction == ORDER_DESC:
        block_number = block_number + 1 if block_number is not None else None
        tx_index = tx_index + 1 if tx_index is not None else None

    if event_index is None and block_number is not None:
        event_index = make_event_index(block_number=block_number, transaction_index=tx_index or 0, item_index=0)

        if ordering.direction == ORDER_DESC:
            event_index -= 1

    if event_index is not None:
        query = query.where(
            ordering.operator_or_equal(wallet_events_t.c.event_index, event_index)
        )

    return query.order_by(*ordering.columns).limit(limit)


def get_wallet_events_ordering(scheme: OrderScheme, direction: OrderDirection):
    columns = {
        ORDER_SCHEME_BY_NUMBER: [wallet_events_t.c.block_number, wallet_events_t.c.event_index],
        ORDER_SCHEME_BY_TIMESTAMP: [wallet_events_t.c.event_index]
    }[scheme]
    return get_ordering(columns, scheme, direction)


get_eth_transfers_by_address_query = functools.partial(get_wallet_events_query, type_=WalletEventType.ETH_TRANSFER)
