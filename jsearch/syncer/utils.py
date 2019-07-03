from aiopg.sa import SAConnection

from jsearch.common.processing.wallet import AssetBalanceUpdate
from jsearch.syncer.database_queries.contracts import increase_erc20_balance_increase_error_count_query


async def report_erc20_balance_of_error(connection: SAConnection, update: AssetBalanceUpdate) -> None:
    query = increase_erc20_balance_increase_error_count_query(contract_address=update.asset_address)
    await connection.execute(query)
