from sqlalchemy import select, desc, and_, or_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Query
from typing import Set

from jsearch.common.tables import erc20_balance_requests_t
from jsearch.syncer.structs import TokenHolder
from jsearch.typing import TokenAddress, AccountAddress


def insert_balance_request_query(
        token_address: TokenAddress,
        account_address: AccountAddress,
        balance: int,
        block_number: int
) -> Query:
    query = insert(erc20_balance_requests_t).values({
        'token_address': token_address,
        'account_address': account_address,
        'block_number': block_number,
        'balance': balance
    })
    query = query.on_conflict_do_update(
        index_elements=[
            erc20_balance_requests_t.c.token_address,
            erc20_balance_requests_t.c.account_address,
        ],
        set_={
            'balance': balance,
            'block_number': block_number,
        },
        where=erc20_balance_requests_t.c.block_number < query.excluded.block_number,
    )

    return query


def get_balance_request_query(holders: Set[TokenHolder]) -> Query:
    query = select(
        columns=[
            erc20_balance_requests_t.c.account_address,
            erc20_balance_requests_t.c.token_address,
            erc20_balance_requests_t.c.balance,
            erc20_balance_requests_t.c.block_number
        ]
    ).order_by(
        erc20_balance_requests_t.c.token_address,
        erc20_balance_requests_t.c.account_address,
        desc(erc20_balance_requests_t.c.block_number),
    ).where(
        or_(*(and_(erc20_balance_requests_t.c.account_address == holder.account,
                   erc20_balance_requests_t.c.token_address == holder.token) for holder in holders))
    )

    return query
