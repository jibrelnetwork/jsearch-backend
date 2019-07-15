from collections import defaultdict

from aiopg.sa import SAConnection
from sqlalchemy.orm import Query
from typing import List, Dict, Any, Tuple, Set

from jsearch.common import contracts
from jsearch.common.processing.decimals_cache import decimals_cache
from jsearch.common.processing.wallet import AssetBalanceUpdates, AssetBalanceUpdate
from jsearch.syncer.database_queries.accounts import (
    get_last_ether_balances_query
)
from jsearch.syncer.database_queries.balance_requests import get_balance_request_query
from jsearch.syncer.database_queries.token_transfers import get_token_address_and_accounts_for_blocks_q, \
    get_incomes_after_block_query, get_outcomes_after_block_query
from jsearch.syncer.structs import TokenHolder, BalanceOnBlock
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


async def get_changes_after_block_query(connection: SAConnection, query: Query) -> int:
    changes = 0
    async with connection.execute(query) as cursor:
        result = await cursor.fetchone()
        if result:
            changes = result.change

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


async def get_balances_on_last_request(connection, holders: Set[TokenHolder]) -> Dict[TokenHolder, BalanceOnBlock]:
    balances = {}
    query = get_balance_request_query(holders)
    async with connection.execute(query) as cursor:
        for item in await cursor.fetchall():
            holder = TokenHolder(token=item.token_address, account=item.account_address)
            balances[holder] = BalanceOnBlock(block=item.block_number, balance=item.balance)
    return balances


async def filter_negative_balances(updates: AssetBalanceUpdates) -> AssetBalanceUpdates:
    safe_token_holder_updates = []
    for update in updates:
        if update.balance < 0:
            update = AssetBalanceUpdate(**{
                **update._asdict(),
                **{'balance': 0}
            })
        safe_token_holder_updates.append(update)
    return safe_token_holder_updates


async def get_token_balance_updates(
        connection: SAConnection,
        last_block: int,
        token_holders: Set[TokenHolder],
) -> AssetBalanceUpdates:
    tokens, holders = split_token_and_holders(token_holders)
    decimals_map = await decimals_cache.get_many(addresses=tokens)

    balances = await get_balances_on_last_request(connection, token_holders)

    last_balance_block = 0
    if balances:
        last_balance_block = max([balance.block for balance in balances.values()])

    balance_changes = await get_balances_changes_after_block(
        connection,
        addresses=holders,
        block=last_balance_block
    )

    return await get_assets_updates(token_holders, decimals_map, balances, balance_changes, last_block)


async def get_assets_updates(
        token_holders: Set[TokenHolder],
        decimals_map: Dict[AccountAddress, int],
        balances: Dict[TokenHolder, BalanceOnBlock],
        balance_changes: Dict[TokenHolder, int],
        last_block: int
) -> AssetBalanceUpdates:
    updates = []
    for holder in token_holders:
        balance_from_request = balances[holder].balance if holder in balances else 0
        changes_from_logs = balance_changes.get(holder) or 0

        balance = int(balance_from_request + changes_from_logs)

        update = AssetBalanceUpdate(
            account_address=holder.account,
            asset_address=holder.token,
            balance=balance,
            block_number=last_block,
            decimals=decimals_map[holder.token],
            nonce=None,
        )
        updates.append(update)
    return updates
