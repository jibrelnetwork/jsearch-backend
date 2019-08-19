from sqlalchemy import select
from sqlalchemy.orm import Query
from typing import Optional

from jsearch.api.database_queries.transactions import get_ordering
from jsearch.api.ordering import Ordering
from jsearch.common.tables import token_holders_t
from jsearch.typing import Columns, OrderScheme, OrderDirection, TokenAddress


def get_default_fields():
    return [
        token_holders_t.c.id,
        token_holders_t.c.account_address,
        token_holders_t.c.token_address,
        token_holders_t.c.balance,
        token_holders_t.c.decimals,
        token_holders_t.c.block_number
    ]


def get_token_holders_ordering(scheme: OrderScheme, direction: OrderDirection) -> Ordering:
    columns: Columns = [
        token_holders_t.c.balance,
        token_holders_t.c.id
    ]
    return get_ordering(columns, scheme, direction)


def get_token_holders_query(
        limit: int,
        ordering: Ordering,
        token_address: TokenAddress,
        balance: Optional[int] = None,
        _id: Optional[int] = None,
) -> Query:
    query = select(
        columns=get_default_fields(),
        whereclause=token_holders_t.c.token_address == token_address,
    )

    if balance is not None:
        query = query.where(
            ordering.operator_or_equal(token_holders_t.c.balance, balance),
        )

    if _id is not None:
        query = query.where(
            ordering.operator_or_equal(token_holders_t.c.id, _id)
        )

    return query.order_by(*ordering.columns).limit(limit)
