import pytest
from sqlalchemy import select
from sqlalchemy.engine import Connection
from typing import List

from jsearch.common.tables import (
    blocks_t
)


@pytest.fixture()
def link_txs_with_block(db: Connection):
    def link(tx_hashes: List[str], block_hash: str) -> List[str]:
        get_txs_query = select([blocks_t.c.transactions]).where(blocks_t.c.hash == block_hash)

        result = db.execute(get_txs_query)

        txs = result.fetchone()['transactions'] or []
        txs = [*txs, *tx_hashes]
        txs = [tx for tx in txs if tx]

        update_txs_query = blocks_t.update(
            whereclause=blocks_t.c.hash == block_hash,
            values={'transactions': txs}
        )
        db.execute(update_txs_query)

        return txs

    return link
