from sqlalchemy import select, false, desc
from sqlalchemy.sql import Select
from typing import List

from aiopg.sa import Engine

from jsearch.common.db import fetch_all
from jsearch.common.structs import Block
from jsearch.common.tables import blocks_t


async def get_canonical_holes(engine: Engine, depth: int) -> List[Block]:
    canonical_holes = []
    canonical_blocks = await get_canonical_blocks(engine, limit=depth)

    for previous, current in zip(canonical_blocks, canonical_blocks[1:]):
        if previous.hash != current.parent_hash:
            canonical_holes.append(current)

    return canonical_holes


async def get_canonical_blocks(engine: Engine, limit: int) -> List[Block]:
    query = get_canonical_blocks_query(limit)

    rows = await fetch_all(engine, query)
    rows.reverse()
    rows = [Block(**r) for r in rows]

    return rows


def get_canonical_blocks_query(limit: int) -> Select:
    return select(
        columns=[
            blocks_t.c.number,
            blocks_t.c.hash,
            blocks_t.c.parent_hash,
        ],
        whereclause=blocks_t.c.is_forked == false()
    ).order_by(desc(blocks_t.c.number)).limit(limit)
