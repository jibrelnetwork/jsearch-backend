from functools import reduce
from typing import Optional, List

from sqlalchemy import select, and_, false, tuple_, exists
from sqlalchemy.dialects.postgresql import array, Any
from sqlalchemy.orm import Query
from sqlalchemy.sql import ClauseElement
from sqlalchemy.sql.functions import max as max_sql

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


def get_last_token_holders_query(account_address: str, token_addresses: List[str]) -> ClauseElement:
    sub_query = select(
        [
            token_holders_t.c.account_address,
            token_holders_t.c.token_address,
            max_sql(token_holders_t.c.block_number)
        ]
    ).where(
        and_(
            token_holders_t.c.account_address == account_address,
            Any(token_holders_t.c.token_address, array(tuple(token_addresses))),
            token_holders_t.c.is_forked == false()
        )
    ).group_by(
        token_holders_t.c.account_address,
        token_holders_t.c.token_address
    ).alias('latest_blocks')

    return select([
        token_holders_t.c.account_address,
        token_holders_t.c.token_address,
        token_holders_t.c.balance,
        token_holders_t.c.block_number,
        token_holders_t.c.block_hash,
        token_holders_t.c.decimals
    ]).where(
        and_(
            tuple_(
                token_holders_t.c.account_address,
                token_holders_t.c.token_address,
                token_holders_t.c.block_number
            ).in_(
                sub_query
            ),
            token_holders_t.c.is_forked == false(),
            token_holders_t.c.balance > 0,

        )
    )


def get_token_holders_query(
        limit: int,
        ordering: Ordering,
        token_address: TokenAddress,
        holder_threshold: Optional[int],
        balance: Optional[int] = None,
        _id: Optional[int] = None,
) -> Query:
    query = select(columns=get_default_fields())

    conditions = [
        token_holders_t.c.token_address == token_address,
        token_holders_t.c.is_forked == false()
    ]

    if holder_threshold:
        conditions.append(
            token_holders_t.c.balance >= holder_threshold,
        )

    if balance is not None and holder_threshold is not None and balance < holder_threshold:
        balance = holder_threshold

    if balance is not None and _id is not None:
        filter_by_id = ordering.operator_or_equal(
            tuple_(
                token_holders_t.c.balance,
                token_holders_t.c.id
            ),
            (balance, _id)
        )
        conditions.append(filter_by_id)

    elif balance is not None:
        filter_by_balance = ordering.operator_or_equal(token_holders_t.c.balance, balance)
        conditions.append(filter_by_balance)

    subquery_table = token_holders_t.alias('source')
    subquery = ~exists().where(
        and_(
            subquery_table.c.account_address == token_holders_t.c.account_address,
            subquery_table.c.token_address == token_holders_t.c.token_address,
            subquery_table.c.block_number > token_holders_t.c.block_number,
            subquery_table.c.is_forked == false()
        )
    )
    conditions.append(subquery)

    query = query.where(reduce(and_, conditions))
    query = query.order_by(*ordering.columns).limit(limit)
    return query
