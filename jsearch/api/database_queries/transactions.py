from sqlalchemy import select
from sqlalchemy.orm import Query

from jsearch.common.tables import transactions_t


def get_tx_hashes_by_block_hash(block_hash: str) -> Query:
    return select(
        columns=[transactions_t.c.hash],
        whereclause=transactions_t.c.block_hash == block_hash,
    ).order_by(
        transactions_t.c.transaction_index
    )
