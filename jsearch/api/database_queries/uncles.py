from typing import List, Optional

from sqlalchemy import select, false, and_
from sqlalchemy.orm import Query

from jsearch.common.tables import uncles_t
from jsearch.api.database_queries.transactions import get_ordering
from jsearch.api.ordering import ORDER_DESC, ORDER_SCHEME_BY_NUMBER, ORDER_SCHEME_BY_TIMESTAMP, Ordering
from jsearch.typing import Columns, OrderScheme, OrderDirection


def get_default_fields():
    return [
        uncles_t.c.difficulty,
        uncles_t.c.extra_data,
        uncles_t.c.gas_limit,
        uncles_t.c.gas_used,
        uncles_t.c.hash,
        uncles_t.c.logs_bloom,
        uncles_t.c.miner,
        uncles_t.c.mix_hash,
        uncles_t.c.nonce,
        uncles_t.c.number,
        uncles_t.c.block_number,
        uncles_t.c.block_hash,
        uncles_t.c.parent_hash,
        uncles_t.c.receipts_root,
        uncles_t.c.sha3_uncles,
        uncles_t.c.size,
        uncles_t.c.state_root,
        uncles_t.c.timestamp,
        uncles_t.c.total_difficulty,
        uncles_t.c.transactions_root,
        uncles_t.c.reward,
    ]


def get_uncles_ordering(scheme: OrderScheme, direction: OrderDirection) -> Ordering:
    columns: Columns = {
        ORDER_SCHEME_BY_NUMBER: [uncles_t.c.number],
        ORDER_SCHEME_BY_TIMESTAMP: [uncles_t.c.timestamp]
    }[scheme]
    return get_ordering(columns, scheme, direction)


def get_uncles_query(
        limit: int,
        order: Ordering,
        columns: Optional[Columns] = None,
        address: Optional[str] = None
) -> Query:
    columns = columns or get_default_fields()
    return select(
        columns=columns,
        whereclause=and_(
            uncles_t.c.is_forked == false(),
            uncles_t.c.miner == address,
        ) if address else uncles_t.c.is_forked == false(),
    ) \
        .order_by(*order.columns) \
        .limit(limit)


def get_uncles_by_number_query(
        limit: int,
        number: int,
        order: Ordering,
        columns: Optional[Columns] = None
) -> Query:
    query = get_uncles_query(limit=limit, order=order, columns=columns)

    if order.direction == ORDER_DESC:
        query = query.where(uncles_t.c.number <= number)
    else:
        query = query.where(uncles_t.c.number >= number)

    return query


def get_uncles_by_timestamp_query(
        limit: int,
        timestamp: int,
        order: Ordering,
        columns: Optional[Columns] = None
) -> Query:
    query = get_uncles_query(limit=limit, order=order, columns=columns)

    if order.direction == ORDER_DESC:
        query = query.where(uncles_t.c.timestamp <= timestamp)
    else:
        query = query.where(uncles_t.c.timestamp >= timestamp)

    return query


def get_uncles_by_miner_address_and_number_query(
        address: str,
        limit: int,
        number: int,
        order: Ordering,
        columns: Optional[Columns] = None
) -> Query:
    query = get_uncles_query(address=address, limit=limit, order=order, columns=columns)

    if order.direction == ORDER_DESC:
        query = query.where(uncles_t.c.number <= number)
    else:
        query = query.where(uncles_t.c.number >= number)

    return query


def get_uncles_by_miner_address_and_timestamp_query(
        address: str,
        limit: int,
        timestamp: int,
        order: Ordering,
        columns: Optional[Columns] = None
) -> Query:
    query = get_uncles_query(address=address, limit=limit, order=order, columns=columns)

    if order.direction == ORDER_DESC:
        query = query.where(uncles_t.c.timestamp <= timestamp)
    else:
        query = query.where(uncles_t.c.timestamp >= timestamp)

    return query


def get_uncle_hashes_by_block_hash_query(block_hash: str) -> Query:
    return select(
        columns=[uncles_t.c.hash],
        whereclause=uncles_t.c.block_hash == block_hash,
    )


def get_uncle_hashes_by_block_hashes_query(block_hashes: List[str]) -> Query:
    return select(
        columns=[uncles_t.c.block_hash, uncles_t.c.hash],
        whereclause=uncles_t.c.block_hash.in_(block_hashes),
    ).order_by(uncles_t.c.block_hash).distinct()
