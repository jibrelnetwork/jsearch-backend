from typing import List

from sqlalchemy import select, Column, and_, false
from sqlalchemy.orm import Query

from jsearch.api.helpers import get_order
from jsearch.common.tables import internal_transactions_t, transactions_t


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


def get_internal_txs_by_account(account: str, order: str, columns: List[Column] = None) -> Query:
    query = select(
        columns=columns or get_default_fields(),
        whereclause=internal_transactions_t.c.is_forked == false(),
    ).select_from(
        internal_transactions_t.join(
            transactions_t,
            and_(
                internal_transactions_t.c.parent_tx_hash == transactions_t.c.hash,
                transactions_t.c.address == account,
            ),
            isouter=True,
        )
    )

    return query.order_by(
        *get_order(
            [
                internal_transactions_t.c.block_number,
                internal_transactions_t.c.parent_tx_hash,
                internal_transactions_t.c.transaction_index,
            ],
            order
        )
    )
