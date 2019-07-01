from collections import defaultdict

from aiopg.sa import SAConnection
from sqlalchemy.orm import Query
from typing import List, Dict, Any, Optional, Tuple, Set

from jsearch.common import contracts
from jsearch.common.processing.decimals_cache import decimals_cache
from jsearch.common.processing.wallet import get_balance_updates, AssetBalanceUpdates, AssetBalanceUpdate
from jsearch.syncer.database_queries.accounts import get_last_ether_balances_query
from jsearch.syncer.database_queries.token_transfers import (
    get_incomes_after_block_query,
    get_outcomes_after_block_query,
    get_token_address_and_accounts_for_blocks_q
)


async def get_last_ether_states_for_addresses_in_blocks(
        connection: SAConnection,
        blocks_hashes: List[str]
) -> List[Dict[str, Any]]:
    """
    Args:
        connection: connection to db
        blocks_hashes: list of blocks hashes

    Returns:
        last states for addresses affected by blocks
    """
    query = get_last_ether_balances_query(blocks_hashes)
    async with connection.execute(query) as cursor:
        results = await cursor.fetchall()

    return results


async def get_changes_map(connection: SAConnection, query: Query) -> Dict[str, Dict[str, int]]:
    async with connection.execute(query) as cursor:
        changes = await cursor.fetchall()

    changes_map = defaultdict(dict)
    for item in changes:
        changes_map[item.token_address][item.account_address] = item.change

    return changes_map


async def get_balances_changes_after_block(
        connection: SAConnection,
        addresses: List[str],
        block: int
) -> Dict[Tuple[str, str], int]:
    incomes_query = get_incomes_after_block_query(addresses, block=block)
    outcomes_query = get_outcomes_after_block_query(addresses, block=block)

    incomes = await get_changes_map(connection, query=incomes_query)
    outcomes = await get_changes_map(connection, query=outcomes_query)

    changes = defaultdict(lambda: 0)

    for token, token_changes in incomes.items():
        for address, value in token_changes.items():
            key = (token, address)
            changes[key] += value

    for token, token_changes in outcomes.items():
        for address, value in token_changes.items():
            key = (token, address)
            changes[key] -= value

    return changes


async def get_token_holders(connection: SAConnection, blocks_hashes: List[str]) -> Set[Tuple[str, str]]:
    query = get_token_address_and_accounts_for_blocks_q(blocks_hashes)

    token_holders = set()

    async with connection.execute(query) as cursor:
        query_result = await cursor.fetchall()

    for item in query_result:
        token = item['token_address']
        owner = item['address']

        token_holders.add((owner, token))

    return token_holders


def get_token_holders_from_transfers(transfers: List[Dict[str, Any]]) -> Set[Tuple[str, str]]:
    holders = set()
    for transfer in transfers:
        to_address = transfer['to_address']
        from_address = transfer['from_address']
        token_address = transfer['token_address']

        if to_address != contracts.NULL_ADDRESS:
            holders.add((to_address, token_address))

        if from_address != contracts.NULL_ADDRESS:
            holders.add((from_address, token_address))

    return holders


def split_token_and_holders(token_holders: Set[Tuple[str, str]]) -> Tuple[List[str], List[str]]:
    tokens = set()
    holders = set()

    for holder, token in token_holders:
        tokens.add(token)
        holders.add(holder)

    return list(tokens), list(holders)


def token_balance_changes_from_transfers(
        transfers: List[Dict[str, Any]],
        token_holder_changes: AssetBalanceUpdates
) -> AssetBalanceUpdates:
    # if this block is last block or one from last block range
    # we need to calculate updates with change from
    # this block transfers
    token_updates_map = {(x.asset_address, x.account_address): x for x in token_holder_changes}
    for transfer in transfers:
        token_address = transfer['token_address']
        account_address = transfer['address']
        status = transfer['status']

        if status and account_address != contracts.NULL_ADDRESS:
            key = token_address, account_address
            update = token_updates_map[key]
            if account_address == transfer['to_address']:
                balance_change = update.balance + transfer['token_value']
            else:
                balance_change = update.balance - transfer['token_value']

            update_data = {**update._asdict(), **{'balance': update.balance + balance_change}}
            token_updates_map[key] = AssetBalanceUpdate(**update_data)
    return list(token_updates_map.values())


async def get_token_balance_updates(
        connection: SAConnection,
        token_holders: Set[Tuple[str, str]],
        decimals_map: Optional[Dict[str, int]] = None,
        block: Optional[int] = None
) -> AssetBalanceUpdates:
    tokens, holders = split_token_and_holders(token_holders)

    if decimals_map is None:
        decimals_map = await decimals_cache.get_many(addresses=tokens)

    token_balance_updates = await get_balance_updates(
        holders=token_holders,
        decimals_map=decimals_map,
        block=block
    )

    if block is not None:
        # if we want to request balance with offset on some block
        # we need to get changes from database since this block
        balance_changes = await get_balances_changes_after_block(connection, addresses=holders, block=block)

        token_updates_map = {(x.asset_address, x.account_address): x for x in token_balance_updates}
        for key, balance_change in balance_changes:
            update = token_updates_map[key]
            update_data = {**update._asdict(), **{'balance': update.balance + balance_change}}
            token_updates_map[key] = AssetBalanceUpdate(**update_data)

    return token_balance_updates
