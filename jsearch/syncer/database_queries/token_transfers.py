from sqlalchemy import select, func, and_, false
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


def get_transfers_to_query(token: str, account: str, from_block: int) -> Query:
    return select([
        func.sum(token_transfers_t.c.token_value).label('value'),
    ]).where(
        and_(
            token_transfers_t.c.token_address == token,
            token_transfers_t.c.address == account,
            token_transfers_t.c.to_address == account,
            token_transfers_t.c.block_number > from_block,
            token_transfers_t.c.is_forked == false(),
            token_transfers_t.c.status == 1,
        )
    )


def get_transfers_from_query(token: str, account: str, from_block: int) -> Query:
    return select([
        func.sum(token_transfers_t.c.token_value).label('value'),
    ]).where(
        and_(
            token_transfers_t.c.token_address == token,
            token_transfers_t.c.address == account,
            token_transfers_t.c.from_address == account,
            token_transfers_t.c.block_number > from_block,
            token_transfers_t.c.is_forked == false(),
            token_transfers_t.c.status == 1,
        )
    )
