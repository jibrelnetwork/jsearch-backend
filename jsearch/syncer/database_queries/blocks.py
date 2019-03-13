from typing import List

from jsearch.common.tables import blocks_t


def get_blocks_by_hashes_query(hashes: List[str]):
    return blocks_t.select().where(blocks_t.c.hash.in_(hashes))
