from typing import List

from sqlalchemy import select, Column
from sqlalchemy.orm import Query

from jsearch.common.tables import internal_transactions_t


def get_default_fields() -> List[Column]:
    return [
        internal_transactions_t.c.block_number,
        internal_transactions_t.c.block_hash,
        internal_transactions_t.c.parent_tx_hash,
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


def get_internal_txs_by_parent(parent_tx_hash: str, order: str, columns: List[Column] = None) -> Query:
    query = select(
        columns=columns or get_default_fields(),
        whereclause=internal_transactions_t.c.parent_tx_hash == parent_tx_hash,
    )

    return _order_query(query, order)


def _order_query(query: Query, direction: str) -> Query:
    if direction == 'asc':
        return query.order_by(
            internal_transactions_t.c.block_number.asc(),
            internal_transactions_t.c.transaction_index.asc(),
        )

    return query.order_by(
        internal_transactions_t.c.block_number.desc(),
        internal_transactions_t.c.transaction_index.desc(),
    )
