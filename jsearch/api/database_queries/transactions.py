from typing import List

from sqlalchemy import select
from sqlalchemy.orm import Query

from jsearch.common.tables import transactions_t


def get_tx_hashes_by_block_hash_query(block_hash: str) -> Query:
    return select(
        columns=[transactions_t.c.hash],
        whereclause=transactions_t.c.block_hash == block_hash,
    ).order_by(
        transactions_t.c.transaction_index
    )


def get_tx_hashes_by_block_hashes_query(block_hashes: List[str]) -> Query:
    return select(
        columns=[transactions_t.c.block_hash, transactions_t.c.hash],
        whereclause=transactions_t.c.block_hash.in_(block_hashes),
    ).order_by(transactions_t.c.block_hash)
