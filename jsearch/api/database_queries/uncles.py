from sqlalchemy import select
from sqlalchemy.orm import Query

from jsearch.common.tables import uncles_t


def get_uncle_hashes_by_block_number(block_hash: int) -> Query:
    return select(
        columns=[uncles_t.c.hash],
        whereclause=uncles_t.c.block_hash == block_hash,
    )
