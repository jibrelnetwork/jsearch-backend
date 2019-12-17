from sqlalchemy import select, Column, and_, false, tuple_, union
from sqlalchemy.orm import Query
from sqlalchemy.sql import CompoundSelect
from sqlalchemy.sql.elements import ClauseList
from typing import List, Optional

from jsearch.api.helpers import get_order
from jsearch.api.ordering import ORDER_SCHEME_BY_NUMBER, ORDER_SCHEME_BY_TIMESTAMP, get_ordering, Ordering
from jsearch.common.tables import internal_transactions_t
from jsearch.typing import OrderScheme, OrderDirection, Columns


def get_default_fields() -> List[Column]:
    return [
        internal_transactions_t.c.block_number,
        internal_transactions_t.c.block_hash,
        internal_transactions_t.c.parent_tx_hash,
        internal_transactions_t.c.parent_tx_index,
        internal_transactions_t.c.op,
        internal_transactions_t.c.call_depth,
        internal_transactions_t.c.timestamp,
        getattr(internal_transactions_t.c, 'from'),
        internal_transactions_t.c.to,
        internal_transactions_t.c.value,
        internal_transactions_t.c.gas_limit,
        internal_transactions_t.c.payload,
        internal_transactions_t.c.status,
        internal_transactions_t.c.transaction_index,
    ]


def get_internal_txs_ordering(scheme: OrderScheme, direction: OrderDirection) -> Ordering:
    columns: Columns = {
        ORDER_SCHEME_BY_NUMBER: [
            internal_transactions_t.c.block_number,
            internal_transactions_t.c.parent_tx_index,
            internal_transactions_t.c.transaction_index,
        ],
        ORDER_SCHEME_BY_TIMESTAMP: [
            internal_transactions_t.c.timestamp,
            internal_transactions_t.c.parent_tx_index,
            internal_transactions_t.c.transaction_index
        ]
    }[scheme]
    return get_ordering(columns, scheme, direction)


def get_internal_txs_by_parent(parent_tx_hash: str, order: str, columns: List[Column] = None) -> Query:
    query = select(
        columns=columns or get_default_fields(),
        whereclause=and_(
            internal_transactions_t.c.parent_tx_hash == parent_tx_hash,
            internal_transactions_t.c.is_forked == false(),
        )
    )

    return query.order_by(
        *get_order(
            [
                internal_transactions_t.c.block_hash,
                internal_transactions_t.c.parent_tx_hash,
                internal_transactions_t.c.transaction_index,
            ],
            order
        )
    )


def get_internal_txs_union_by_from_and_to(
        query: Query,
        address: str,
        order_by: Columns,
        limit: int,
        filter_q: Optional[ClauseList] = None
) -> CompoundSelect:
    filter_by_from_q = getattr(internal_transactions_t.c, 'from') == address
    filter_by_to_q = getattr(internal_transactions_t.c, 'to') == address

    if filter_q is not None:
        filter_by_from_q &= filter_q
        filter_by_to_q &= filter_q

    return union(
        query.where(filter_by_from_q).order_by(*order_by).limit(limit).alias("from"),
        query.where(filter_by_to_q).order_by(*order_by).limit(limit).alias("to"),
    )


def get_internal_txs_by_address_and_block_query(
        limit: int,
        address: str,
        block_number: int,
        ordering: Ordering,
        tx_index: Optional[int] = None,
        parent_tx_index: Optional[int] = None,
) -> Query:
    query = select(
        columns=get_default_fields(),
    ).where(internal_transactions_t.c.is_forked == false())

    columns = []
    params = []
    if block_number is not None and block_number != 'latest':
        columns.append(internal_transactions_t.c.block_number)
        params.append(block_number)
    if parent_tx_index is not None:
        columns.append(internal_transactions_t.c.parent_tx_index)
        params.append(parent_tx_index)
    if tx_index is not None:
        columns.append(internal_transactions_t.c.transaction_index)
        params.append(tx_index)
    if columns:
        filter_q = ordering.operator_or_equal(tuple_(*columns), tuple_(*params))
    else:
        filter_q = None

    query = get_internal_txs_union_by_from_and_to(query, address, ordering.columns, limit, filter_q)
    return query.order_by(*ordering.get_ordering_for_union_query(query)).limit(limit)


def get_internal_txs_by_address_and_timestamp_query(
        limit: int,
        address: str,
        timestamp: int,
        ordering: Ordering,
        tx_index: Optional[int] = None,
        parent_tx_index: Optional[int] = None,
) -> Query:
    query = select(
        columns=get_default_fields(),
    ).where(internal_transactions_t.c.is_forked == false())

    if parent_tx_index is None and tx_index is None:
        filter_q = ordering.operator_or_equal(internal_transactions_t.c.timestamp, timestamp)
    elif tx_index is None:
        filter_q = ordering.operator_or_equal(
            tuple_(
                internal_transactions_t.c.timestamp,
                internal_transactions_t.c.parent_tx_index,
            ),
            (
                timestamp,
                parent_tx_index,
            )
        )
    else:
        filter_q = ordering.operator_or_equal(
            tuple_(
                internal_transactions_t.c.timestamp,
                internal_transactions_t.c.parent_tx_index,
                internal_transactions_t.c.transaction_index
            ),
            (
                timestamp,
                parent_tx_index,
                tx_index
            )
        ).self_group()

    query = get_internal_txs_union_by_from_and_to(query, address, ordering.columns, limit, filter_q)
    return query.order_by(*ordering.get_ordering_for_union_query(query)).limit(limit)
