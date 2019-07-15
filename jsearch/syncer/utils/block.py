import asyncio
import logging
from collections import defaultdict

import time
from aiopg.sa import Engine
from functools import partial
from sqlalchemy.orm import Query
from typing import Dict, Tuple, Set

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

logger = logging.getLogger()


async def get_changes_for_token_holder(
        engine: Engine,
        query: Query,
        holder: TokenHolder,
        multiplier: int = 1
) -> Tuple[TokenHolder, int]:
    async with engine.acquire() as connection:
        changes = await get_changes_after_block_query(connection, query)
    return holder, changes * multiplier


get_positive_changes_for_token_holder = partial(get_changes_for_token_holder, multiplier=1)
get_negative_changes_for_token_holder = partial(get_changes_for_token_holder, multiplier=-1)


async def get_balances_changes_after_block(
        engine: Engine,
        holders: Set[TokenHolder],
        since_block: Dict[TokenHolder, int]
) -> Dict[TokenHolder, int]:
    changes = defaultdict(lambda: 0)

    tasks = []
    for holder in holders:
        block = since_block.get(holder, 0)

        incomes_query = get_incomes_after_block_query(holder, block=block)
        outcomes_query = get_outcomes_after_block_query(holder, block=block)

        tasks.append(get_positive_changes_for_token_holder(engine, incomes_query, holder))
        tasks.append(get_negative_changes_for_token_holder(engine, outcomes_query, holder))

    for holder, value in await asyncio.gather(*tasks):
        changes[holder] += value

    return changes


async def get_token_balance_updates(
        engine: Engine,
        last_block: int,
        token_holders: Set[TokenHolder],
) -> AssetBalanceUpdates:
    tokens, holders = split_token_and_holders(token_holders)
    decimals_map = await decimals_cache.get_many(addresses=tokens)

    start_time = time.monotonic()
    async with engine.acquire() as connection:
        balances = await get_balances_on_last_request(connection, token_holders)

    on_last_balance_time = time.monotonic() - start_time

    balance_changes = await get_balances_changes_after_block(
        engine,
        holders=token_holders,
        since_block={holder: balance_on_block.block for holder, balance_on_block in balances.items()}
    )

    logger.info("Db queries", {
        "on_last_request": on_last_balance_time,
        'balance_changes_time': time.monotonic() - on_last_balance_time
    })

    return await get_assets_updates(token_holders, decimals_map, balances, balance_changes, last_block)
