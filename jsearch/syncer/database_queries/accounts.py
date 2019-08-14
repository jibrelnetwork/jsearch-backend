from sqlalchemy import select
from sqlalchemy.orm import Query
from typing import List

from jsearch.common.tables import accounts_state_t


def get_accounts_state_for_blocks_query(blocks_hashes: List[str]) -> Query:
    return select(
        columns=[
            accounts_state_t.c.address,
        ],
    ).where(accounts_state_t.c.block_hash.in_(blocks_hashes))
