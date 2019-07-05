from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Query

from jsearch.common.tables import erc20_errors
from jsearch.typing import TokenAddress, AccountAddress


def increase_erc20_balance_increase_error_count_query(
        contract_address: TokenAddress,
        account_address: AccountAddress,
        block_number: int
) -> Query:
    query = insert(erc20_errors).values({
        'contract_address': contract_address,
        'account_address': account_address,
        'block_number': block_number,
        'errors': 1
    })
    query = query.on_conflict_do_update(
        index_elements=[
            erc20_errors.c.contract_address,
            erc20_errors.c.account_address
        ],
        set_={
            'errors': query.excluded.errors + 1,
            'block_number': block_number,
        }
    )

    return query
