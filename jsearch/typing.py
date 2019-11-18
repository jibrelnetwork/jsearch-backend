from sqlalchemy import Column
from typing import Any, Dict, Callable, Awaitable, Coroutine, List, Union, Optional

AnyDict = Dict[str, Any]
AnyDicts = List[AnyDict]

Abi = List[Dict[str, Any]]
Abi_ERC20 = Abi

IntOrStr = Union[int, str]

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

BlockHash = str
BlockHashes = List[BlockHash]

TokenAddress = str
TokenAddresses = List[TokenAddress]

TokenHolderUpdate = Dict[str, Any]
TokenHolderUpdates = List[TokenHolderUpdate]

AccountAddress = str
AccountAddresses = List[AccountAddress]

Columns = List[Column]

OrderDirection = str
OrderScheme = str

LastAffectedBlock = Optional[int]
ProgressPercent = float
BlockchainTipAsDict = Dict[str, Any]
