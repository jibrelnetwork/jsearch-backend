from sqlalchemy import Column, select, and_, false, tuple_
from sqlalchemy.orm import Query
from typing import List, Optional

from jsearch.api.ordering import Ordering, ORDER_SCHEME_BY_NUMBER, ORDER_SCHEME_BY_TIMESTAMP, get_ordering
from jsearch.common.tables import token_transfers_t
from jsearch.typing import OrderScheme, OrderDirection, Columns


def get_default_fields() -> List[Column]:
    return [
        token_transfers_t.c.transaction_hash,
        token_transfers_t.c.transaction_index,
        token_transfers_t.c.log_index,
        token_transfers_t.c.block_number,
        token_transfers_t.c.block_hash,
        token_transfers_t.c.timestamp,
        token_transfers_t.c.from_address,
        token_transfers_t.c.to_address,
        token_transfers_t.c.token_address,
        token_transfers_t.c.token_value,
        token_transfers_t.c.token_decimals,
        token_transfers_t.c.token_name,
        token_transfers_t.c.token_symbol,
    ]


def get_transfers_ordering(scheme: OrderScheme, direction: OrderDirection) -> Ordering:
    columns: Columns = {
        ORDER_SCHEME_BY_NUMBER: [
            token_transfers_t.c.block_number,
            token_transfers_t.c.transaction_index,
            token_transfers_t.c.log_index,
        ],
        ORDER_SCHEME_BY_TIMESTAMP: [
            token_transfers_t.c.timestamp,
            token_transfers_t.c.transaction_index,
            token_transfers_t.c.log_index,
        ]
    }[scheme]
    return get_ordering(columns, scheme, direction)


def get_transfers_by_address_query(address: str, ordering: Ordering) -> Query:
    return select(
        columns=get_default_fields(),
        whereclause=and_(
            token_transfers_t.c.address == address,
            token_transfers_t.c.is_forked == false(),
        )
    ).order_by(*ordering.columns)


def get_transfers_by_token_query(address: str, ordering: Ordering) -> Query:
    return select(
        columns=get_default_fields(),
        whereclause=and_(
            token_transfers_t.c.token_address == address,
            token_transfers_t.c.is_forked == false(),
        )
    ).order_by(*ordering.columns)


def get_paginated_query_by_block_number(
        query: Query,
        limit: int,
        block_number: int,
        ordering: Ordering,
        log_index: Optional[int] = None,
        transaction_index: Optional[int] = None,
) -> Query:
    if transaction_index is None and log_index is None:
        q = ordering.operator_or_equal(token_transfers_t.c.block_number, block_number)
    elif log_index is None:
        q = ordering.operator_or_equal(
            tuple_(
                token_transfers_t.c.block_number,
                token_transfers_t.c.transaction_index,
            ),
            (
                block_number,
                transaction_index,
            )
        )
    else:
        q = ordering.operator_or_equal(
            tuple_(
                token_transfers_t.c.block_number,
                token_transfers_t.c.transaction_index,
                token_transfers_t.c.log_index
            ),
            (
                block_number,
                transaction_index,
                log_index
            )
        ).self_group()

    query = query.where(q).limit(limit)
    return query


def get_token_transfers_by_account_and_block_number(
        address: str,
        limit: int,
        block_number: int,
        ordering: Ordering,
        transaction_index: Optional[int] = None,
        log_index: Optional[int] = None,
) -> Query:
    query = get_transfers_by_address_query(address, ordering)
    return get_paginated_query_by_block_number(query, limit, block_number, ordering, transaction_index, log_index)


def get_token_transfers_by_token_and_block_number(
        address: str,
        limit: int,
        block_number: int,
        ordering: Ordering,
        transaction_index: Optional[int] = None,
        log_index: Optional[int] = None,
) -> Query:
    query = get_transfers_by_token_query(address, ordering)
    return get_paginated_query_by_block_number(query, limit, block_number, ordering, transaction_index, log_index)
