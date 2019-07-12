from typing import Any, Dict, NewType, Callable, Awaitable, Coroutine, List, TypeVar

Abi = List[Dict[str, Any]]
Abi_ERC20 = NewType('Abi_ERC20', Abi)

Account = Dict[str, Any]
Accounts = List[Account]

Log = Dict[str, Any]
Logs = List[Log]

Block = Dict[str, Any]
Blocks = List[Block]

Transfer = Dict[str, Any]
Transfers = List[Transfer]

EventArgs = Dict[str, Any]
Contract = Dict[str, Any]
Contracts = List[Contract]
Token = Dict[str, Any]

AsyncCallback = Callable[..., Awaitable[Any]]
AsyncReducer = Callable[[Any, AsyncCallback], Any]

Event = Dict[str, Any]
Transaction = Dict[str, Any]
InternalTransaction = Dict[str, Any]
PendingTransaction = Dict[str, Any]
PendingTransactions = List[PendingTransaction]

AssetUpdate = Dict[str, Any]
AssetUpdates = List[AssetUpdate]
AssetTransfer = Dict[str, Any]

AnyCoroutine = Coroutine[Any, Any, Any]
AnyCoroutineMaker = Callable[..., AnyCoroutine]

TokenAddress = TypeVar('TokenAddress', bound=str)
TokenAddresses = List[TokenAddress]

AccountAddress = TypeVar('AccountAddress', bound=str)
AccountAddresses = List[AccountAddress]
