import logging

from asyncpg.pool import Pool

import asyncpgsa
from sqlalchemy.orm import Query

from jsearch.common.types import Rows

logger = logging.getLogger(__name__)


async def fetch(pool: Pool, saquery: Query) -> Rows:
    async with pool.acquire() as conn:
        query, params = asyncpgsa.compile_query(saquery)

        logger.debug('Compiled a query', extra={'query': query, 'params': params})

        rows = await conn.fetch(query, *params)
        rows = [dict(r) for r in rows]

    return rows


def in_app_distinct(rows: Rows) -> Rows:
    """
    There're cases when `SELECT DISTINCT` slows down DB performance so much,
    that removing duplicates in-app is faster, than in a DB.
    """
    rows_distinct = list()
    distinct_keys = set()

    for row in rows:
        distinct_key = tuple(row.values())

        if distinct_key in distinct_keys:
            continue

        distinct_keys.add(distinct_key)
        rows_distinct.append(row)

    return rows_distinct
