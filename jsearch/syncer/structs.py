from typing import NamedTuple

from jsearch.typing import TokenAddress, AccountAddress


class TokenHolder(NamedTuple):
    token: TokenAddress
    account: AccountAddress

