from sqlalchemy import select, func, and_
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


def get_transfer_to_since_block_query(address: str, block_number: int) -> Query:
    return select([
        func.sum(token_transfers_t.c.token_value).label('value'),
    ]).where(
        and_(
            token_transfers_t.c.to_address == address,
            block_number > block_number
        )
    )


def get_transfer_from_since_block_query(address: str, block_number: int) -> Query:
    return select([
        func.sum(token_transfers_t.c.token_value).label('value'),
    ]).where(
        and_(
            token_transfers_t.c.from_address == address,
            block_number > block_number
        )
    )
