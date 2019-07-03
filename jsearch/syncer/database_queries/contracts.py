from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Query

from jsearch.common.tables import erc20_contracts
from jsearch.typing import TokenAddress


def increase_erc20_balance_increase_error_count_query(contract_address: TokenAddress) -> Query:
    query = insert(erc20_contracts).values({'contract_address': contract_address, 'errors': 1})
    query = query.on_conflict_do_update(
        index_elements=[erc20_contracts.c.contract_address],
        set_={
            'errors': query.excluded.errors + 1
        }
    )

    return query
