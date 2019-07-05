from collections import defaultdict

from aiopg.sa import SAConnection, Engine
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
from jsearch.syncer.structs import TokenHolder
from jsearch.syncer.utils import get_last_block_with_offset, report_erc20_balance_of_error
from jsearch.typing import TokenAddresses, AccountAddresses, AccountAddress, TokenAddress


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


async def get_changes_map(connection: SAConnection, query: Query) -> Dict[TokenHolder, int]:
    async with connection.execute(query) as cursor:
        changes = await cursor.fetchall()

    return {TokenHolder(item.token_address, item.address): item.change for item in changes if item.address}


async def get_balances_changes_after_block(
        connection: SAConnection,
        addresses: List[str],
        block: int
) -> Dict[TokenHolder, int]:
    incomes_query = get_incomes_after_block_query(addresses, block=block)
    outcomes_query = get_outcomes_after_block_query(addresses, block=block)

    incomes = await get_changes_map(connection, query=incomes_query)
    outcomes = await get_changes_map(connection, query=outcomes_query)

    changes = defaultdict(lambda: 0)

    for holder, value in incomes.items():
        changes[holder] += value

    for holder, value in outcomes.items():
        changes[holder] -= value

    return changes


async def get_token_holders(connection: SAConnection, blocks_hashes: List[str]) -> Set[TokenHolder]:
    query = get_token_address_and_accounts_for_blocks_q(blocks_hashes)

    token_holders = set()

    async with connection.execute(query) as cursor:
        query_result = await cursor.fetchall()

    for item in query_result:
        token: TokenAddress = item['token_address']
        owner: AccountAddress = item['address']

        token_holders.add(TokenHolder(token, owner))

    return token_holders


def get_token_holders_from_transfers(transfers: List[Dict[str, Any]]) -> Set[TokenHolder]:
    holders = set()
    for transfer in transfers:
        to_address: AccountAddress = transfer['to_address']
        from_address: AccountAddress = transfer['from_address']
        token_address: TokenAddress = transfer['token_address']

        if to_address != contracts.NULL_ADDRESS:
            holders.add(TokenHolder(token_address, to_address))

        if from_address != contracts.NULL_ADDRESS:
            holders.add(TokenHolder(token_address, from_address))

    return holders


def split_token_and_holders(token_holders: Set[TokenHolder]) -> Tuple[TokenAddresses, AccountAddresses]:
    tokens = set()
    holders = set()

    for token, holder in token_holders:
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
    token_updates_map = {TokenHolder(x.asset_address, x.account_address): x for x in token_holder_changes}
    for transfer in transfers:
        token_address: TokenAddress = transfer['token_address']
        account_address: AccountAddress = transfer['address']
        status = transfer['status']

        if status and account_address != contracts.NULL_ADDRESS and transfer['from_address'] != transfer['to_address']:
            key = TokenHolder(token_address, account_address)
            update = token_updates_map[key]

            if account_address == transfer['to_address']:
                balance = update.balance + transfer['token_value']
            else:
                balance = update.balance - transfer['token_value']

            update_data = {**update._asdict(), **{'balance': int(balance)}}
            token_updates_map[key] = AssetBalanceUpdate(**update_data)
    return list(token_updates_map.values())


async def get_token_balance_updates(
        connection: SAConnection,
        last_block: int,
        token_holders: Set[TokenHolder],
        decimals_map: Optional[Dict[str, int]] = None,
        use_offset: bool = False,
) -> AssetBalanceUpdates:
    tokens, holders = split_token_and_holders(token_holders)

    if decimals_map is None:
        decimals_map = await decimals_cache.get_many(addresses=tokens)

    token_balance_updates = await get_balance_updates(
        holders=token_holders,
        decimals_map=decimals_map,
        last_block=last_block,
        use_offset=use_offset
    )

    if use_offset:
        # if we want to request balance with offset on some block
        # we need to get changes from database since this block

        last_block_with_offset = get_last_block_with_offset(last_block)
        balance_changes = await get_balances_changes_after_block(
            connection,
            addresses=holders,
            block=last_block_with_offset
        )
        token_updates_map = {TokenHolder(x.asset_address, x.account_address): x for x in token_balance_updates}
        for key, balance_change in balance_changes.items():
            update = token_updates_map.get(key)
            if update:
                update_data = {**update._asdict(), **{'balance': int(update.balance + balance_change)}}
                token_updates_map[key] = AssetBalanceUpdate(**update_data)

        token_balance_updates = list(token_updates_map.values())

    return token_balance_updates


async def filter_negative_balances(engine: Engine, updates: AssetBalanceUpdates) -> AssetBalanceUpdates:
    token_updates = [update for update in updates if update.balance > 0]
    safe_token_holder_updates = []
    for update in token_updates:
        if update.balance < 0:
            async with engine.acquire() as connection:
                await report_erc20_balance_of_error(
                    connection=connection,
                    contract_address=update.asset_address,
                    account_address=update.asset_address,
                    block_number=update.block_number
                )
        else:
            safe_token_holder_updates.append(update)
    return safe_token_holder_updates
