from sqlalchemy import select, Column, and_, false, union
from sqlalchemy.orm import Query
from typing import List, Dict, Optional

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
    columns: Dict[OrderScheme, Columns] = {
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
    }
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


def get_internal_txs_by_address_query(address: str, ordering: Ordering) -> Query:
    return select(
        columns=get_default_fields(),
        whereclause=and_(
            internal_transactions_t.c.is_forked == false(),
            internal_transactions_t.c.tx_origin == address,
        )
    ).order_by(*ordering.columns)


def get_internal_txs_by_address_and_block_query(
        limit: int,
        address: str,
        block_number: int,
        ordering: Ordering,
        tx_index: Optional[int] = None,
        parent_tx_index: Optional[int] = None,
) -> Query:
    query = get_internal_txs_by_address_query(address, ordering)

    if parent_tx_index is None and tx_index is None:
        query = query.where(ordering.operator_or_equal(internal_transactions_t.c.block_number, block_number))
    elif tx_index is None:
        query = union(
            query.where(
                ordering.operator(internal_transactions_t.c.block_number, block_number),
            ).limit(limit).alias('after_block'),
            query.where(
                and_(
                    internal_transactions_t.c.block_number == block_number,
                    ordering.operator_or_equal(internal_transactions_t.c.parent_tx_index, parent_tx_index),
                )
            ).limit(limit).alias('after_parent_tx'),
        )
        query = query.order_by(*ordering.get_ordering_for_union_query(query))
    else:
        query = union(
            query.where(
                ordering.operator(internal_transactions_t.c.block_number, block_number),
            ).limit(limit).alias('after_block'),
            query.where(
                and_(
                    internal_transactions_t.c.block_number == block_number,
                    ordering.operator(internal_transactions_t.c.parent_tx_index, parent_tx_index),
                )
            ).limit(limit).alias('after_parent_tx'),
            query.where(
                and_(
                    internal_transactions_t.c.block_number == block_number,
                    internal_transactions_t.c.parent_tx_index == parent_tx_index,
                    ordering.operator_or_equal(internal_transactions_t.c.transaction_index, tx_index),
                )
            ).limit(limit).alias('after_tx')
        )
        query = query.order_by(*ordering.get_ordering_for_union_query(query))
    query = query.limit(limit)
    return query


def get_internal_txs_by_address_and_timestamp_query(
        limit: int,
        address: str,
        timestamp: int,
        ordering: Ordering,
        tx_index: Optional[int] = None,
        parent_tx_index: Optional[int] = None,
) -> Query:
    query = get_internal_txs_by_address_query(address, ordering)

    if parent_tx_index is None and tx_index is None:
        query = query.where(ordering.operator_or_equal(internal_transactions_t.c.timestamp, timestamp))
    elif tx_index is None:
        query = union(
            query.where(
                ordering.operator(internal_transactions_t.c.timestamp, timestamp),
            ).limit(limit).alias('after_block'),
            query.where(
                and_(
                    internal_transactions_t.c.timestamp == timestamp,
                    ordering.operator_or_equal(internal_transactions_t.c.parent_tx_index, parent_tx_index),
                )
            ).limit(limit).alias('after_parent_tx'),
        )
        query = query.order_by(*ordering.get_ordering_for_union_query(query))
    else:
        query = union(
            query.where(
                ordering.operator(internal_transactions_t.c.timestamp, timestamp),
            ).limit(limit).alias('after_block'),
            query.where(
                and_(
                    internal_transactions_t.c.timestamp == timestamp,
                    ordering.operator(internal_transactions_t.c.parent_tx_index, parent_tx_index),
                )
            ).limit(limit).alias('after_parent_tx'),
            query.where(
                and_(
                    internal_transactions_t.c.timestamp == timestamp,
                    internal_transactions_t.c.parent_tx_index == parent_tx_index,
                    ordering.operator_or_equal(internal_transactions_t.c.transaction_index, tx_index),
                )
            ).limit(limit).alias('after_tx')
        ).order_by(*ordering.columns)
        query = query.order_by(*ordering.get_ordering_for_union_query(query))

    query = query.limit(limit)
    return query
