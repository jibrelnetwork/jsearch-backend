from sqlalchemy import and_, false, Column, select, desc, asc
from sqlalchemy.orm import Query
from typing import List, Optional

from jsearch.api.helpers import get_order, ORDER_ASC, ORDER_DESC
from jsearch.api.structs import Ordering
from jsearch.common.tables import blocks_t
from jsearch.typing import Columns, OrderScheme, OrderDirection

DIRECTIONS = {
    ORDER_ASC: asc,
    ORDER_DESC: desc
}


def get_default_fields():
    return [
        blocks_t.c.difficulty,
        blocks_t.c.extra_data,
        blocks_t.c.gas_limit,
        blocks_t.c.gas_used,
        blocks_t.c.hash,
        blocks_t.c.transactions,
        blocks_t.c.uncles,
        blocks_t.c.logs_bloom,
        blocks_t.c.miner,
        blocks_t.c.mix_hash,
        blocks_t.c.nonce,
        blocks_t.c.number,
        blocks_t.c.parent_hash,
        blocks_t.c.receipts_root,
        blocks_t.c.sha3_uncles,
        blocks_t.c.state_root,
        blocks_t.c.static_reward,
        blocks_t.c.timestamp,
        blocks_t.c.transactions_root,
        blocks_t.c.uncle_inclusion_reward,
        blocks_t.c.tx_fees,
    ]


ORDER_SCHEME_BY_NUMBER: OrderScheme = 'order_by_number'
ORDER_SCHEME_BY_TIMESTAMP: OrderScheme = 'order_by_timestamp'


def get_order_schema(timestamp: Optional[int]) -> OrderScheme:
    if timestamp is None:
        return ORDER_SCHEME_BY_NUMBER

    return ORDER_SCHEME_BY_TIMESTAMP


def get_ordering(scheme: OrderScheme, direction=OrderDirection) -> Ordering:
    columns = {
        ORDER_SCHEME_BY_NUMBER: [blocks_t.c.number],
        ORDER_SCHEME_BY_TIMESTAMP: {blocks_t.c.timestamp}
    }[scheme]

    direction_func = DIRECTIONS[direction]

    return Ordering(
        columns=[direction_func(column) for column in columns],
        fields=[column.name for column in columns],
        scheme=scheme,
        direction=direction
    )


def get_blocks_query(
        limit: int,
        order: Ordering,
        columns: Optional[Columns] = None
) -> Query:
    columns = columns or get_default_fields()
    return select(
        columns=columns,
        whereclause=blocks_t.c.is_forked == false(),
    ) \
        .order_by(*order.columns) \
        .limit(limit)


def get_blocks_by_number_query(
        limit: int,
        number: int,
        order: Ordering,
        columns: Optional[Columns] = None
) -> Query:
    query = get_blocks_query(limit, order=order, columns=columns)

    if order.direction == ORDER_DESC:
        query = query.where(blocks_t.c.number <= number)
    else:
        query = query.where(blocks_t.c.number >= number)

    return query


def get_blocks_by_timestamp_query(
        limit: int,
        timestamp: int,
        order: Ordering,
        columns: Optional[Columns] = None
) -> Query:
    query = get_blocks_query(limit, order=order, columns=columns)

    if order.direction == ORDER_DESC:
        query = query.where(blocks_t.c.timestamp <= timestamp)
    else:
        query = query.where(blocks_t.c.timestamp >= timestamp)

    return query


def get_mined_blocks_query(miner: str,
                           limit: int,
                           offset: int,
                           order: List[Column],
                           direction: Optional[str] = None,
                           columns: List[Column] = None):
    columns = columns or get_default_fields()
    order = get_order(order, direction)
    return select(
        columns=columns,
        whereclause=and_(
            blocks_t.c.miner == miner,
            blocks_t.c.is_forked == false()
        ),
    ) \
        .order_by(*order) \
        .offset(offset) \
        .limit(limit)


def get_block_by_hash_query(block_hash: str, columns: List[Column] = None) -> Query:
    columns = columns or get_default_fields()
    return select(
        columns=columns,
        whereclause=and_(
            blocks_t.c.hash == block_hash,
            blocks_t.c.is_forked == false()
        )
    )


def get_block_by_number_query(number: int, columns: List[Column] = None) -> Query:
    columns = columns or get_default_fields()
    return select(
        columns=columns,
        whereclause=and_(
            blocks_t.c.number == number,
            blocks_t.c.is_forked == false()
        )
    )


def get_last_block_query(columns: List[Column] = None) -> Query:
    columns = columns or get_default_fields()
    return select(
        columns=columns,
        whereclause=blocks_t.c.is_forked == false()
    ).order_by(desc(blocks_t.c.number)).limit(1)


def get_block_number_by_hash_query(block_hash: str) -> Query:
    return select([blocks_t.c.number]).where(blocks_t.c.hash == block_hash)
