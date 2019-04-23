
from typing import Any, List

import aiohttp
import backoff


MAX_RETRY = 5


def _decimal_result(res):
    if 'error' not in res:
        try:
            res['result'] = str(int(res['result'], 16))
        except Exception:
            pass
    return res


class NodeProxy:

    def __init__(self, node_url):
        self.node_url = node_url

    @backoff.on_exception(backoff.expo, aiohttp.ClientError, max_tries=MAX_RETRY)
    async def _request(self, method: str, params: List = None):
        params = params or []
        message = {'jsonrpc': '2.0', 'method': method, 'params': params, 'id': 1}
        async with aiohttp.ClientSession() as session:
            async with session.post(self.node_url, json=message) as resp:
                return await resp.json()

    async def client_version(self) -> str:
        res = await self._request('web3_clientVersion')
        return res

    async def gas_price(self) -> str:
        res = await self._request('eth_gasPrice')
        res = _decimal_result(res)
        return res

    async def call_contract(self, args: List) -> Any:
        res = await self._request('eth_call', args)
        return res

    async def estimate_gas(self, args: List) -> str:
        res = await self._request('eth_estimateGas', args)
        res = _decimal_result(res)
        return res

    async def send_raw_transaction(self, args: List) -> str:
        res = await self._request('eth_sendRawTransaction', args)
        return res

    async def transaction_count(self, args: List) -> str:
        res = await self._request('eth_getTransactionCount', args)
        res = _decimal_result(res)
        return res
