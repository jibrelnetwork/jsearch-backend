from typing import List

from sqlalchemy import select, Column, and_, false, or_
from sqlalchemy.orm import Query

from jsearch.api.helpers import get_order
from jsearch.common.tables import pending_transactions_t


def get_default_fields() -> List[Column]:
    return [
        pending_transactions_t.c.hash,
        pending_transactions_t.c.status,
        pending_transactions_t.c.removed,
        pending_transactions_t.c.r,
        pending_transactions_t.c.s,
        pending_transactions_t.c.v,
        pending_transactions_t.c.to,
        getattr(pending_transactions_t.c, 'from'),
        pending_transactions_t.c.gas,
        pending_transactions_t.c.gas_price,
        pending_transactions_t.c.input,
        pending_transactions_t.c.nonce,
        pending_transactions_t.c.value,
    ]


def get_pending_txs_by_account(account: str, order: str, columns: List[Column] = None) -> Query:
    query = select(
        columns=columns or get_default_fields(),
        whereclause=and_(
            pending_transactions_t.c.removed.is_(false()),
            or_(
                getattr(pending_transactions_t.c, 'to') == account,
                getattr(pending_transactions_t.c, 'from') == account,
            )
        ),
    )

    return query.order_by(
        *get_order(
            [
                pending_transactions_t.c.nonce,
                pending_transactions_t.c.timestamp,
            ],
            order,
        )
    )
