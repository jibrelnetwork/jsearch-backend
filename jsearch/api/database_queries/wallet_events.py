from sqlalchemy import select, Column, and_, false
from sqlalchemy.orm import Query
from typing import List, Optional

from jsearch.api.ordering import Ordering, ORDER_SCHEME_BY_NUMBER, ORDER_SCHEME_BY_TIMESTAMP, get_ordering, ORDER_DESC
from jsearch.common.tables import wallet_events_t
from jsearch.common.wallet_events import make_event_index
from jsearch.typing import OrderScheme, OrderDirection, Columns


def get_default_fields() -> List[Column]:
    return [
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

    if ordering.direction == ORDER_DESC:
        block_number = block_number is not None and block_number + 1
        tx_index = tx_index is not None and tx_index + 1

    if event_index is None:
        event_index = make_event_index(block_number=block_number, transaction_index=tx_index or 0, item_index=0)

        if ordering.direction == ORDER_DESC:
            event_index -= 1

    return query.where(
        ordering.operator_or_equal(wallet_events_t.c.event_index, event_index)
    ).order_by(*ordering.columns).limit(limit)
