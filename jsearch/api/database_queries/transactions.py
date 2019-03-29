from typing import List

from sqlalchemy import select, Column
from sqlalchemy.orm import Query

from jsearch.common.tables import transactions_t


def get_default_fields():
    return [
        transactions_t.c.block_hash,
        transactions_t.c.block_number,
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
    ]


def get_tx_hashes_by_block_hash(block_hash: str) -> Query:
    return select(
        columns=[transactions_t.c.hash],
        whereclause=transactions_t.c.block_hash == block_hash,
    ).order_by(
        transactions_t.c.transaction_index
    )


def get_tx_by_address(address: str, order: str, columns: List[Column] = None) -> Query:
    query = select(
        columns=columns or get_default_fields(),
        whereclause=transactions_t.c.address == address
    )

    return _order_tx_query(query, order)


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
