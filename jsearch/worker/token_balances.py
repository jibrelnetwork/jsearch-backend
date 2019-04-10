import asyncio

from aiopg.sa import SAConnection

from jsearch import settings
from jsearch.common.contracts import ERC20_ABI, ERC20_DEFAULT_DECIMALS
from jsearch.common.processing.erc20_balances import (
    BalanceUpdate,
    BalanceUpdates,
    fetch_erc20_balance_bulk
)
from jsearch.common.processing.utils import fetch_contracts, prefetch_decimals
from jsearch.multiprocessing import executor
from jsearch.syncer.database import MainDBSync
from jsearch.syncer.database_queries.token_transfers import get_token_address_and_accounts_for_block_q
from jsearch.utils import split


async def get_balance_updates(connection: SAConnection, block_hash: str, block_number: int) -> BalanceUpdates:
    loop = asyncio.get_event_loop()
    query = get_token_address_and_accounts_for_block_q(block_hash=block_hash)

    updates = set()
    async with connection.execute(query) as cursor:
        for record in await cursor.fetchall():
            token = record['token_address']
            account = record['address']

            update = BalanceUpdate(
                token_address=token,
                account_address=account,
                block=block_number,
                abi=None,
                decimals=None
            )
            updates.add(update)

        addresses = list({update.token_address for update in updates})

        contracts = await fetch_contracts(addresses)
        contracts = await loop.run_in_executor(executor.get(), prefetch_decimals, contracts)

        for update in updates:
            contract = contracts.get(update.token_address)
            if contract:
                update.abi = contract['abi']
                update.decimals = contract['decimals']
            else:
                update.abi = ERC20_ABI
                update.decimals = ERC20_DEFAULT_DECIMALS

        return [update for update in updates if update.abi is not None]


def update_balances(updates: BalanceUpdates,
                    last_block: int,
                    batch_size: int = settings.ETH_NODE_BATCH_REQUEST_SIZE) -> BalanceUpdates:
    with MainDBSync(settings.JSEARCH_MAIN_DB) as db:

        for chunk in split(updates, batch_size):
            updates = fetch_erc20_balance_bulk(chunk, block=last_block)

        for update in updates:
            update.apply(db, last_block)

    return updates
