from sqlalchemy import select
from sqlalchemy.orm import Query

from jsearch.common.tables import token_transfers_t


def get_token_address_and_accounts_for_block_q(block_hash: str) -> Query:
    return select(
        columns=[
            token_transfers_t.c.token_address,
            token_transfers_t.c.address
        ],
        distinct=True
    ).where(token_transfers_t.c.block_hash == block_hash)
