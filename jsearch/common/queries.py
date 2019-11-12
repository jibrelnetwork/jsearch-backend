import logging
from typing import Tuple, Set

from jsearch.common.types import Rows

logger = logging.getLogger(__name__)


def in_app_distinct(rows: Rows) -> Rows:
    """
    There're cases when `SELECT DISTINCT` slows down DB performance so much,
    that removing duplicates in-app is faster, than in a DB.
    """
    rows_distinct = list()
    distinct_keys: Set[Tuple] = set()

    for row in rows:
        distinct_key = tuple(row.values())

        if distinct_key in distinct_keys:
            continue

        distinct_keys.add(distinct_key)
        rows_distinct.append(row)

    return rows_distinct
