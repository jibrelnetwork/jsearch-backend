from sqlalchemy import select, Column, desc
from sqlalchemy.orm import Query
from typing import List, Optional

from jsearch.api.ordering import ORDER_ASC
from jsearch.common.tables import logs_t


def get_default_fields() -> List[Column]:
    return [
        logs_t.c.address,
        logs_t.c.block_hash,
        logs_t.c.block_number,
        logs_t.c.timestamp,
        logs_t.c.data,
        logs_t.c.log_index,
        logs_t.c.removed,
        logs_t.c.topics,
        logs_t.c.transaction_hash,
        logs_t.c.transaction_index,
    ]


def _order_query(query: Query, direction: str) -> Query:
    if direction == ORDER_ASC:
        return query.order_by(
            logs_t.c.block_number,
            logs_t.c.transaction_index,
            logs_t.c.log_index,
        )
    return query.order_by(
        desc(logs_t.c.block_number),
        desc(logs_t.c.transaction_index),
        desc(logs_t.c.log_index),
    )


def get_logs_by_address_query(
        address: str,
        order: str,
        limit: int,
        offset: int,
        block_from: Optional[int] = None,
        block_until: Optional[int] = None,
        columns: List[Column] = None
) -> Query:
    query = select(
        columns=columns or get_default_fields(),
        whereclause=logs_t.c.address == address,
    ).limit(limit)

    if offset is not None:
        query = query.offset(offset)

    if block_from is not None:
        query = query.where(logs_t.c.block_number >= block_from)

    if block_until is not None:
        query = query.where(logs_t.c.block_number <= block_until)

    return _order_query(query, order)
