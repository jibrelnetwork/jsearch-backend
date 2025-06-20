from sqlalchemy import and_, false, Column, select, desc
from sqlalchemy.orm import Query
from typing import List, Optional

from jsearch.api.database_queries.transactions import get_ordering
from jsearch.api.ordering import (
    ORDER_DESC,
    ORDER_SCHEME_BY_NUMBER,
    ORDER_SCHEME_BY_TIMESTAMP,
    Ordering,
    DIRECTIONS,
    DIRECTIONS_OPERATOR_OR_EQUAL_MAPS
)
from jsearch.common.tables import blocks_t
from jsearch.typing import Columns, OrderScheme, OrderDirection


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


def get_blocks_ordering(scheme: OrderScheme, direction: OrderDirection) -> Ordering:
    columns: Columns = {
        ORDER_SCHEME_BY_NUMBER: [blocks_t.c.number],
        ORDER_SCHEME_BY_TIMESTAMP: [blocks_t.c.timestamp]
    }[scheme]
    return get_ordering(columns, scheme, direction)


def get_blocks_query(
        order: Ordering,
        limit: Optional[int] = None,
        miner: Optional[str] = None,
        columns: Optional[Columns] = None,
) -> Query:
    columns = columns or get_default_fields()
    query = select(
        columns=columns,
        whereclause=blocks_t.c.is_forked == false(),
    ) \
        .order_by(*order.columns)

    if limit:
        query = query.limit(limit)

    if miner is not None:
        query = query.where(blocks_t.c.miner == miner)
    return query


def get_blocks_by_number_query(
        limit: Optional[int],
        number: int,
        order: Ordering,
        miner: Optional[str] = None,
        columns: Optional[Columns] = None,
) -> Query:
    query = get_blocks_query(limit=limit, order=order, miner=miner, columns=columns)

    if order.direction == ORDER_DESC:
        query = query.where(blocks_t.c.number <= number)
    else:
        query = query.where(blocks_t.c.number >= number)

    return query


def get_blocks_by_timestamp_query(
        limit: Optional[int],
        timestamp: int,
        order: Ordering,
        miner: Optional[str] = None,
        columns: Optional[Columns] = None,
) -> Query:
    query = get_blocks_query(limit=limit, order=order, miner=miner, columns=columns)

    if order.direction == ORDER_DESC:
        query = query.where(blocks_t.c.timestamp <= timestamp)
    else:
        query = query.where(blocks_t.c.timestamp >= timestamp)

    return query


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
    return select(
        [
            blocks_t.c.number,
            blocks_t.c.timestamp,
            blocks_t.c.is_forked
        ]
    ).where(blocks_t.c.hash == block_hash)


def get_block_number_by_timestamp_query(timestamp: int, order_direction: OrderDirection) -> Query:
    direction_func = DIRECTIONS[order_direction]
    operator_or_equal = DIRECTIONS_OPERATOR_OR_EQUAL_MAPS[order_direction]

    return select([blocks_t.c.number, blocks_t.c.hash, blocks_t.c.timestamp]).where(
        and_(
            operator_or_equal(blocks_t.c.timestamp, timestamp),
            blocks_t.c.is_forked == false(),
        )
    ).order_by(direction_func(blocks_t.c.timestamp)).limit(1)


def generate_blocks_query(
        order: Ordering,
        number: Optional[int] = None,
        timestamp: Optional[int] = None,
        limit: Optional[int] = None,
) -> Query:
    if number is None and timestamp is None:
        query = get_blocks_query(limit=limit, order=order)
    else:
        if order.scheme == ORDER_SCHEME_BY_TIMESTAMP:
            query = get_blocks_by_timestamp_query(limit=limit, timestamp=timestamp, order=order)  # type: ignore

        elif order.scheme == ORDER_SCHEME_BY_NUMBER:
            query = get_blocks_by_number_query(limit=limit, number=number, order=order)  # type: ignore
        else:
            raise ValueError('Invalid scheme: {scheme}')
    return query
