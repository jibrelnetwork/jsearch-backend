from typing import Any, Dict, NewType, List

Abi = List[Dict[str, Any]]
Abi_ERC20 = NewType('Abi_ERC20', Abi)

Log = Dict[str, Any]
EventArgs = Dict[str, Any]
Contract = Dict[str, Any]

