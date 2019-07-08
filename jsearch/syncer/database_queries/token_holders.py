from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Query
from typing import Optional

from jsearch.common.tables import token_holders_t
from jsearch.typing import AccountAddress, TokenAddress


def upsert_token_holder_balance_q(token_address: TokenAddress,
                                  account_address: AccountAddress,
                                  balance: int,
                                  block_number: int,
                                  decimals: Optional[int] = None) -> Query:
    insert_query = insert(token_holders_t).values(
        token_address=token_address,
        account_address=account_address,
        balance=balance,
        decimals=decimals,
        block_number=block_number
    )
    return insert_query.on_conflict_do_update(
        index_elements=['token_address', 'account_address'],
        set_={
            'balance': balance,
            'block_number': block_number
        },
        where=token_holders_t.c.block_number <= insert_query.excluded.block_number
    )
