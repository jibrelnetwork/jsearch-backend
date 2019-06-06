from sqlalchemy import select, Column, and_
from sqlalchemy.orm import Query
from typing import List, Optional

from jsearch.api.helpers import get_order
from jsearch.common.tables import wallet_events_t


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
