from typing import Dict, Any
from typing import NamedTuple, List

from jsearch.typing import AnyDicts, AnyDict
from jsearch.typing import TokenAddress, AccountAddress


class TokenHolder(NamedTuple):
    token: TokenAddress
    account: AccountAddress


class TokenHolderBalance(NamedTuple):
    token: TokenAddress
    account: AccountAddress
    balance: int
    decimals: int
    block_hash: str
    block_number: int


TokenHolderBalances = List[TokenHolderBalance]


class BalanceOnBlock(NamedTuple):
    block: int
    balance: int


class RawBlockData(NamedTuple):
    reward: AnyDict
    header: AnyDict
    body: AnyDict

    receipts: AnyDict

    accounts: AnyDicts
    internal_txs: AnyDicts
    token_balances: TokenHolderBalances

    is_forked: bool

    @property
    def block_number(self) -> int:
        return self.header['block_number']

    @property
    def block_hash(self) -> str:
        return self.header['block_hash']

    @property
    def timestamp(self) -> int:
        return int(self.header['fields']['timestamp'], 16)

    @property
    def uncles(self) -> List[Dict[str, Any]]:
        return self.body['fields']['Uncles'] or []

    @property
    def transactions(self) -> List[Dict[str, Any]]:
        return self.body['fields']['Transactions'] or []


class BlockData(NamedTuple):
    accounts: AnyDicts
    assets_summary_updates: AnyDicts
    block: AnyDict
    internal_txs: AnyDicts
    logs: AnyDicts
    receipts: AnyDicts
    token_holders_updates: AnyDicts
    transfers: AnyDicts
    txs: AnyDicts
    uncles: AnyDicts
    wallet_events: AnyDicts
