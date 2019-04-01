from sqlalchemy import Column, select, and_, false
from sqlalchemy.orm import Query
from typing import List

from jsearch.common.tables import token_transfers_t


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


def get_token_transfers_by_token(address: str, order: str, columns: List[Column] = None) -> Query:
    query = select(
        columns=columns or get_default_fields(),
        whereclause=and_(
            token_transfers_t.c.token_address == address,
            token_transfers_t.c.is_forked == false(),
        )
    )

    return order_token_transfers_query(query, order)


def get_token_transfers_by_account(address: str, order: str, columns: List[Column] = None) -> Query:
    query = select(
        columns=columns or get_default_fields(),
        whereclause=and_(
            token_transfers_t.c.address == address,
            token_transfers_t.c.is_forked == false(),
        )
    )

    return order_token_transfers_query(query, order)


def order_token_transfers_query(query: Query, direction: str) -> Query:
    if direction == 'asc':
        return query.order_by(
            token_transfers_t.c.block_number.asc(),
            token_transfers_t.c.transaction_index.asc(),
            token_transfers_t.c.log_index.asc(),
        )

    return query.order_by(
        token_transfers_t.c.block_number.desc(),
        token_transfers_t.c.transaction_index.desc(),
        token_transfers_t.c.log_index.desc(),
    )
