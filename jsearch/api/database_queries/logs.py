from sqlalchemy import select, Column, desc, false, and_, tuple_
from sqlalchemy.orm import Query
from typing import List, Optional, Dict

from jsearch.api.ordering import Ordering, ORDER_SCHEME_BY_NUMBER, ORDER_SCHEME_BY_TIMESTAMP, get_ordering
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


def get_logs_by_address_query(address: str, ordering: Ordering) -> Query:
    return select(
        columns=get_default_fields(),
        whereclause=and_(
            logs_t.c.is_forked == false(),
            logs_t.c.address == address,
        )
    ).order_by(*ordering.columns)


def get_logs_by_address_and_block_query(
        address: str,
        limit: int,
        ordering: Ordering,
        block_number: Optional[int],
        transaction_index: Optional[int],
        log_index: Optional[int],
) -> Query:
    query = get_logs_by_address_query(address, ordering)

    if transaction_index is None and log_index is None:
        q = ordering.operator_or_equal(logs_t.c.block_number, block_number)
    elif log_index is None:
        q = ordering.operator_or_equal(
            tuple_(
                logs_t.c.block_number,
                logs_t.c.transaction_index,
            ),
            (
                block_number,
                transaction_index,
            )
        )
    else:
        q = ordering.operator_or_equal(
            tuple_(
                logs_t.c.block_number,
                logs_t.c.transaction_index,
                logs_t.c.log_index,
            ),
            (
                block_number,
                transaction_index,
                log_index,
            )
        ).self_group()

    return query.where(q).limit(limit)


def get_logs_by_address_and_timestamp_query(
        address: str,
        limit: int,
        ordering: Ordering,
        timestamp: Optional[int],
        transaction_index: Optional[int],
        log_index: Optional[int],
) -> Query:
    query = get_logs_by_address_query(address, ordering)

    if transaction_index is None and log_index is None:
        q = ordering.operator_or_equal(logs_t.c.timestamp, timestamp)
    elif log_index is None:
        q = ordering.operator_or_equal(
            tuple_(
                logs_t.c.timestamp,
                logs_t.c.transaction_index,
            ),
            (
                timestamp,
                transaction_index,
            )
        )
    else:
        q = ordering.operator_or_equal(
            tuple_(
                logs_t.c.timestamp,
                logs_t.c.transaction_index,
                logs_t.c.log_index,
            ),
            (
                timestamp,
                transaction_index,
                log_index,
            )
        ).self_group()

    return query.where(q).limit(limit)
