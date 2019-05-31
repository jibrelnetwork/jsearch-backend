from typing import Any, Dict, NewType, List, Callable, Awaitable, Coroutine

Abi = List[Dict[str, Any]]
Abi_ERC20 = NewType('Abi_ERC20', Abi)

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
AssetTransfer = Dict[str, Any]

AnyCoroutine = Coroutine[Any, None, None]