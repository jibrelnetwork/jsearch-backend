from collections import defaultdict

from aiopg.sa import SAConnection
from typing import Dict, Set

from jsearch.common.processing.decimals_cache import decimals_cache
from jsearch.common.processing.wallet import AssetBalanceUpdates
from jsearch.syncer.database_queries.token_transfers import (
    get_outcomes_after_block_query,
    get_incomes_after_block_query
)
from jsearch.syncer.structs import TokenHolder
from jsearch.syncer.utils.balances import (
    get_changes_after_block_query,
    split_token_and_holders,
    get_balances_on_last_request,
    get_assets_updates
)


async def get_balances_changes_after_block(
        connection: SAConnection,
        holders: Set[TokenHolder],
        since_block: Dict[TokenHolder, int]
) -> Dict[TokenHolder, int]:
    changes = defaultdict(lambda: 0)
    for holder in holders:
        block = since_block.get(holder, 0)

        incomes_query = get_incomes_after_block_query(holder, block=block)
        outcomes_query = get_outcomes_after_block_query(holder, block=block)

        incomes = await get_changes_after_block_query(connection, incomes_query)
        outcomes = await get_changes_after_block_query(connection, outcomes_query)

        changes[holder] += incomes
        changes[holder] -= outcomes

    return changes


async def get_token_balance_updates(
        connection: SAConnection,
        last_block: int,
        token_holders: Set[TokenHolder],
) -> AssetBalanceUpdates:
    tokens, holders = split_token_and_holders(token_holders)
    decimals_map = await decimals_cache.get_many(addresses=tokens)

    balances = dict(await get_balances_on_last_request(connection, token_holders))
    balance_changes = await get_balances_changes_after_block(
        connection,
        holders=token_holders,
        since_block={holder: balance_on_block.block for holder, balance_on_block in balances.items()}
    )

    return await get_assets_updates(token_holders, decimals_map, balances, balance_changes, last_block)
