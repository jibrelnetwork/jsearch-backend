from typing import Dict, Set, Tuple, NamedTuple, List, Optional

from jsearch import settings
from jsearch.common.processing.erc20_balances import get_balances
from jsearch.common.wallet_events import (
    event_from_internal_tx,
    event_from_token_transfer,
    event_from_tx,
    WalletEventType,
)
from jsearch.syncer.database_queries.assets_summary import upsert_assets_summary_query
from jsearch.syncer.database_queries.token_holders import upsert_token_holder_balance_q
from jsearch.typing import Accounts, AssetUpdates

ETHER_ASSET_ADDRESS = ''


class AssetBalanceUpdate(NamedTuple):
    account_address: str
    asset_address: str
    decimals: int
    balance: int

    nonce: Optional[str]

    def as_token_holder_update(self):
        return {
            'token_address': self.asset_address,
            'account_address': self.account_address,
            'balance': self.balance,
            'decimals': self.decimals
        }

    def to_upsert_assets_summary_query(self):
        return upsert_assets_summary_query(
            address=self.account_address,
            asset_address=self.asset_address,
            value=self.balance,
            decimals=self.decimals,
            nonce=self.nonce,
        )

    def to_upsert_token_holder_query(self):
        return upsert_token_holder_balance_q(**self.as_token_holder_update())


AssetBalanceUpdates = List[AssetBalanceUpdate]


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


async def get_balance_updates(
        holders: Set[Tuple[str, str]],
        decimals_map: Dict[str, int],
        block: Optional[int] = None
) -> AssetBalanceUpdates:
    balances = await get_balances(
        owners=list(holders),
        block=block,
        batch_size=settings.ETH_NODE_BATCH_REQUEST_SIZE
    )
    updates = []
    for owner, token, balance in balances:
        update = AssetBalanceUpdate(
            account_address=owner,
            asset_address=token,
            balance=balance,
            nonce=None,
            decimals=decimals_map[token],
        )
        updates.append(update)
    return updates


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
        }
        updates.append(update_data)
    return updates


def assets_from_token_balance_updates(token_balance_updates: AssetBalanceUpdates) -> AssetUpdates:
    updates = []
    for balance in token_balance_updates:
        update_data = {
            'address': balance.account_address,
            'asset_address': balance.asset_address,
            'value': balance.balance,
            'decimals': balance.decimals,
        }
        updates.append(update_data)
    return updates
