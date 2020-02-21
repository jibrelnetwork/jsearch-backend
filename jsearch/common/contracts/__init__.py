from jsearch.typing import AccountAddress

from .dex import DEX_ABI
from .erc20 import ERC20_ABI, ERC20_ABI_SIMPLE, ERC20_DEFAULT_DECIMALS, ERC20_METHODS_IDS, Abi_ERC20  # noqa: F401
from .utils import decode_event  # noqa: F401

NULL_ADDRESS: AccountAddress = '0x0000000000000000000000000000000000000000'
