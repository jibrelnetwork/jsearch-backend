from typing import List

from sqlalchemy import select
from sqlalchemy.orm import Query

from jsearch.common.tables import uncles_t


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
