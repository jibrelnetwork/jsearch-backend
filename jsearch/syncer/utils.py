from aiopg.sa import SAConnection

from jsearch import settings
from jsearch.syncer.database_queries.contracts import increase_erc20_balance_increase_error_count_query
from jsearch.typing import TokenAddress, AccountAddress


async def report_erc20_balance_of_error(
        connection: SAConnection,
        contract_address: TokenAddress,
        account_address: AccountAddress,
        block_number: int,
) -> None:
    query = increase_erc20_balance_increase_error_count_query(
        contract_address=contract_address,
        account_address=account_address,
        block_number=block_number
    )
    await connection.execute(query)


def get_last_block_with_offset(last_block: int) -> int:
    return last_block - settings.ETH_BALANCE_BLOCK_OFFSET
