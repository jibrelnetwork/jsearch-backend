from sqlalchemy import select, Column, and_, false, tuple_
from sqlalchemy.sql.expression import func
from sqlalchemy.orm import Query
from typing import List, Optional

from jsearch.api.ordering import get_ordering, ORDER_SCHEME_BY_NUMBER, ORDER_SCHEME_BY_TIMESTAMP, Ordering
from jsearch.api.helpers import Tag
from jsearch.common.tables import transactions_t
from jsearch.typing import OrderScheme, OrderDirection, Columns


def get_default_fields() -> List[Column]:
    return [
        transactions_t.c.block_hash,
        transactions_t.c.block_number,
        transactions_t.c.timestamp,
        getattr(transactions_t.c, 'from'),
        transactions_t.c.gas,
        transactions_t.c.gas_price,
        transactions_t.c.hash,
        transactions_t.c.input,
        transactions_t.c.nonce,
        transactions_t.c.r,
        transactions_t.c.s,
        transactions_t.c.to,
        transactions_t.c.transaction_index,
        transactions_t.c.v,
        transactions_t.c.value,
        transactions_t.c.status,
    ]


def get_tx_ordering(scheme: OrderScheme, direction: OrderDirection) -> Ordering:
    columns: Columns = {
        ORDER_SCHEME_BY_NUMBER: [transactions_t.c.block_number, transactions_t.c.transaction_index],
        ORDER_SCHEME_BY_TIMESTAMP: [transactions_t.c.timestamp, transactions_t.c.transaction_index]
    }[scheme]
    return get_ordering(columns, scheme, direction)


def get_tx_hashes_by_block_hash_query(block_hash: str) -> Query:
    return select(
        columns=[transactions_t.c.hash],
        whereclause=transactions_t.c.block_hash == block_hash,
    ).order_by(
        transactions_t.c.transaction_index
    )


def get_tx_hashes_by_block_hashes_query(block_hashes: List[str]) -> Query:
    return select(
        columns=[transactions_t.c.block_hash, transactions_t.c.hash, transactions_t.c.transaction_index],
        whereclause=transactions_t.c.block_hash.in_(block_hashes),
    ).order_by(transactions_t.c.transaction_index).distinct()


def get_tx_by_hash(tx_hash: str, columns: List[Column] = None) -> Query:
    return select(
        columns=columns or get_default_fields(),
        whereclause=and_(
            transactions_t.c.is_forked == false(),
            transactions_t.c.hash == tx_hash,
        )
    ).limit(1)


def get_tx_by_address_query(address: str, ordering: Ordering, columns: List[Column] = None) -> Query:
    return select(
        columns=columns or get_default_fields(),
        whereclause=and_(
            transactions_t.c.is_forked == false(),
            transactions_t.c.address == address,
        )
    ).order_by(*ordering.columns)


def get_tx_by_address_and_block_query(
        limit: int,
        address: str,
        block_number: int,
        ordering: Ordering,
        tx_index: Optional[int] = None,
        columns: Optional[Columns] = None
) -> Query:
    query = get_tx_by_address_query(address, ordering, columns)
    columns = []
    params = []
    if block_number is not None:
        columns.append(transactions_t.c.block_number)
        params.append(block_number)
    if tx_index is not None:
        columns.append(transactions_t.c.transaction_index)
        params.append(tx_index)
    if columns:
        q = ordering.operator_or_equal(tuple_(*columns), tuple_(*params))
        return query.where(q).limit(limit)
    else:
        return query.limit(limit)


def get_tx_by_address_and_timestamp_query(
        limit: int,
        address: str,
        timestamp: int,
        ordering: Ordering,
        tx_index: Optional[int] = None,
        columns: Optional[Columns] = None
) -> Query:
    query = get_tx_by_address_query(address, ordering, columns)
    if tx_index is None:
        q = ordering.operator_or_equal(transactions_t.c.timestamp, timestamp)
    else:
        q = ordering.operator_or_equal(
            tuple_(
                transactions_t.c.timestamp,
                transactions_t.c.transaction_index
            ),
            (timestamp, tx_index)
        )
    return query.where(q).limit(limit)


def _order_tx_query(query: Query, direction: str) -> Query:
    if direction == 'asc':
        return query.order_by(
            transactions_t.c.block_number.asc(),
            transactions_t.c.transaction_index.asc(),
        )

    return query.order_by(
        transactions_t.c.block_number.desc(),
        transactions_t.c.transaction_index.desc(),
    )


def get_txs_for_events_query(events_query: Query, order: str, columns: Optional[List[Column]] = None) -> Query:
    columns = columns or get_default_fields()
    query = select(columns).where(transactions_t.c.hash.in_(events_query))
    return _order_tx_query(query, order)


def get_transactions_by_hashes(hashes):
    columns = get_default_fields()
    query = select(columns).where(and_(transactions_t.c.hash.in_(hashes),
                                       transactions_t.c.is_forked == false()))
    return query


def get_block_txs_query(
        tag: Tag,
        tx_index: Optional[int] = None
) -> Query:
    columns = get_default_fields()

    if tag.is_hash():
        query = select(columns).where(and_(transactions_t.c.block_hash == tag.value,
                                           transactions_t.c.is_forked == false()))
    elif tag.is_number():
        query = select(columns).where(and_(transactions_t.c.block_number == tag.value,
                                           transactions_t.c.is_forked == false()))
    else:
        subquery = select([func.max(transactions_t.c.block_number).label('max_block_number')])
        query = select(columns).where(and_(transactions_t.c.block_number.in_(subquery),
                                           transactions_t.c.is_forked == false()))

    if tx_index is not None:
        query = query.where(transactions_t.c.transaction_index == tx_index)

    return query
