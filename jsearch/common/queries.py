import logging

from asyncpg.pool import Pool
from typing import Any, Dict, List

import asyncpgsa
from sqlalchemy.orm import Query

log = logging.getLogger()


async def fetch(pool: Pool, saquery: Query) -> List[Dict[str, Any]]:
    async with pool.acquire() as conn:
        query, params = asyncpgsa.compile_query(saquery)

        log.debug(query)
        log.debug(params)

        rows = await conn.fetch(query, *params)
        rows = [dict(r) for r in rows]

    return rows
