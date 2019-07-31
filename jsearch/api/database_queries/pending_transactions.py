from sqlalchemy import select, Column, and_, false, func
from sqlalchemy import tuple_, union
from sqlalchemy.orm import Query
from sqlalchemy.sql import CompoundSelect
from sqlalchemy.sql.elements import ClauseList
from typing import List, Optional

from jsearch.api.ordering import Ordering, get_ordering
from jsearch.common.tables import pending_transactions_t
from jsearch.typing import OrderScheme, OrderDirection, Columns


def get_default_fields() -> List[Column]:
    return [
        pending_transactions_t.c.id,
        pending_transactions_t.c.timestamp,
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


def get_pending_txs_ordering(scheme: OrderScheme, direction: OrderDirection) -> Ordering:
    columns = [
        pending_transactions_t.c.timestamp,
        pending_transactions_t.c.id,
    ]
    return get_ordering(columns, scheme, direction)


def get_pending_txs_union_by_from_and_to(
        query: Query,
        account: str,
        order_by: Columns,
        limit: int,
        filter_q: Optional[ClauseList] = None
) -> CompoundSelect:
    filter_by_from_q = getattr(pending_transactions_t.c, 'from') == account
    filter_by_to_q = getattr(pending_transactions_t.c, 'to') == account

    if filter_q is not None:
        filter_by_from_q &= filter_q
        filter_by_to_q &= filter_q

    return union(
        query.where(filter_by_from_q).order_by(*order_by).limit(limit).alias("from"),
        query.where(filter_by_to_q).order_by(*order_by).limit(limit).alias("to"),
    )


def get_account_pending_txs_timestamp(account: str, ordering: Ordering) -> CompoundSelect:
    query = select(
        [
            pending_transactions_t.c.timestamp
        ]
    ).where(pending_transactions_t.c.removed == false())

    order_by = [ordering.apply_direction(pending_transactions_t.c.timestamp)]
    query = get_pending_txs_union_by_from_and_to(query, account, order_by=order_by, limit=1)
    query = query.limit(1)

    return query.order_by(ordering.apply_direction(query.c.timestamp)).limit(1)


def get_pending_txs_by_account(
        account: str,
        limit: int,
        ordering: Ordering,
        timestamp: Optional[int] = None,
        id: Optional[int] = None
) -> Query:
    query = select(
        columns=get_default_fields(),
    ).where(pending_transactions_t.c.removed == false())

    if id is not None and timestamp is not None:
        filter_q = ordering.operator_or_equal(
            tuple_(
                pending_transactions_t.c.timestamp,
                pending_transactions_t.c.id
            ),
            (timestamp, id)
        )
    elif timestamp is not None:
        filter_q = ordering.operator_or_equal(pending_transactions_t.c.timestamp, timestamp)
    else:
        filter_q = None

    query = get_pending_txs_union_by_from_and_to(query, account, ordering.columns, limit, filter_q)
    return query.order_by(*ordering.get_ordering_for_union_query(query)).limit(limit)


def get_outcoming_pending_txs_count(account: str) -> Query:
    query = select([func.count(pending_transactions_t.c.last_synced_id)]).where(
        and_(pending_transactions_t.c.removed.is_(false()),
             pending_transactions_t.c['from'] == account)
    )
    return query
