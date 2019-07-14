from aiopg.sa import SAConnection

from jsearch import settings
from jsearch.syncer.database_queries.balance_requests import insert_balance_request_query
from jsearch.typing import TokenAddress, AccountAddress


async def insert_balance_request(
        connection: SAConnection,
        contract_address: TokenAddress,
        account_address: AccountAddress,
        balance: int,
        block_number: int
) -> None:
    query = insert_balance_request_query(
        token_address=contract_address,
        account_address=account_address,
        balance=balance,
        block_number=block_number
    )
    await connection.execute(query)


def get_last_block_with_offset(last_block: int) -> int:
    return last_block - settings.ETH_BALANCE_BLOCK_OFFSET
