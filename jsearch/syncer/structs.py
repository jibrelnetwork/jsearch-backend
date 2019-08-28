from typing import NamedTuple, List

from jsearch.typing import TokenAddress, AccountAddress


class TokenHolder(NamedTuple):
    token: TokenAddress
    account: AccountAddress


class TokenHolderBalance(NamedTuple):
    token: TokenAddress
    account: AccountAddress
    balance: int
    block_hash: str
    block_number: int


TokenHolderBalances = List[TokenHolderBalance]


class BalanceOnBlock(NamedTuple):
    block: int
    balance: int
