from sqlalchemy import select, Column, desc
from sqlalchemy.orm import Query
from typing import List, Optional, Dict

from jsearch.api.ordering import ORDER_ASC, Ordering, ORDER_SCHEME_BY_NUMBER, ORDER_SCHEME_BY_TIMESTAMP, get_ordering
from jsearch.common.tables import logs_t
from jsearch.typing import OrderScheme, OrderDirection, Columns


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


def get_logs_ordering(scheme: OrderScheme, direction: OrderDirection) -> Ordering:
    columns: Dict[OrderScheme, Columns] = {
        ORDER_SCHEME_BY_NUMBER: [
            logs_t.c.block_number,
            logs_t.c.transaction_index,
            logs_t.c.log_index,
        ],
        ORDER_SCHEME_BY_TIMESTAMP: [
            logs_t.c.timestamp,
            logs_t.c.transaction_index,
            logs_t.c.log_index
        ]
    }
    return get_ordering(columns, scheme, direction)


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
