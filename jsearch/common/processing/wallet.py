from typing import Dict

from jsearch.common.wallet_events import (
    event_from_internal_tx,
    event_from_token_transfer,
    event_from_tx,
    WalletEventType,
)
from jsearch.syncer.structs import TokenHolderBalances
from jsearch.typing import Accounts, AssetUpdates, TokenHolderUpdates, TokenHolderUpdate, AssetUpdate

ETHER_ASSET_ADDRESS = ''


def events_from_transactions(transactions, contracts_set, excluded_types=(WalletEventType.ERC20_TRANSFER,)):
    """
    Args:
        transactions: raw txs data
        contracts_set: set of known contracts
        excluded_types: excluded events types

    Notes:
        we excluded by default only erc20-transfer transaction
        we cannot fill such types of events from tx data

        we have another data source for erc20 transfer events -
        table `token_transfers`
    """
    events = (event_from_tx(tx['address'], tx, is_receiver_contract=tx['to'] in contracts_set) for tx in transactions)
    return [event for event in events if event and event['type'] not in excluded_types]


def events_from_transfers(transfers, transactions):
    tx_map = {tx['hash']: tx for tx in transactions}
    events = []
    for t in transfers:
        e = event_from_token_transfer(t['address'], t, tx_map[t['transaction_hash']])
        events.append(e)
    return events


def events_from_internal_transactions(internal_transactions, transactions):
    tx_map = {tx['hash']: tx for tx in transactions}
    events = []
    for it in internal_transactions:
        events.append(event_from_internal_tx(it['from'], it, tx_map[it['parent_tx_hash']]))
        events.append(event_from_internal_tx(it['to'], it, tx_map[it['parent_tx_hash']]))
    return events


def assets_from_accounts(accounts: Accounts) -> AssetUpdates:
    """
     address       | character varying |           | not null |
     asset_address | character varying |           | not null |
     tx_number     | integer           |           |          |
     nonce         | integer           |           |          |
     value         | numeric           |           |          |
     decimals      | integer           |           |          |
    """
    updates = []
    for acc in accounts:
        update_data = {
            'address': acc['address'],
            'asset_address': ETHER_ASSET_ADDRESS,
            'value': acc['balance'],
            'decimals': 0,
            'nonce': acc['nonce'],
            'block_number': acc['block_number'],
            'block_hash': acc['block_hash']
        }
        updates.append(update_data)
    return updates


def token_holders_from_token_balances(
        token_holder_balances: TokenHolderBalances,
        decimals_map: Dict[str, int],
) -> TokenHolderUpdates:
    updates = []
    for token_holder_balance in token_holder_balances:
        decimals = decimals_map[token_holder_balance.token]
        update: TokenHolderUpdate = {
            'decimals': decimals,
            'token_address': token_holder_balance.token,
            'account_address': token_holder_balance.account,
            'balance': token_holder_balance.balance,
            'block_number': token_holder_balance.block_number,
            'block_hash': token_holder_balance.block_hash
        }
        updates.append(update)
    return updates


def asset_records_from_token_balances(
        token_holder_balances: TokenHolderBalances,
        decimals_map: Dict[str, int]
) -> AssetUpdates:
    updates = []
    for token_holder_balance in token_holder_balances:
        decimals = decimals_map[token_holder_balance.token]
        update_data: AssetUpdate = {
            'decimals': decimals,
            'address': token_holder_balance.account,
            'asset_address': token_holder_balance.token,
            'value': token_holder_balance.balance,
            'block_number': token_holder_balance.block_number,
            'block_hash': token_holder_balance.block_hash,
            'tx_number': 1,
        }
        updates.append(update_data)
    return updates
