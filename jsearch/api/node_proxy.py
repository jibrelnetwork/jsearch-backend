
from typing import Any, List

import aiohttp
import backoff


MAX_RETRY = 5


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

    async def gas_price(self) -> str:
        res = await self._request('eth_gasPrice')
        return res

    async def call_contract(self, args: List) -> Any:
        res = await self._request('eth_call', args)
        return res

    async def estimate_gas(self, args: List) -> str:
        res = await self._request('eth_estimateGas', args)
        return res

    async def send_raw_transaction(self, args: List) -> str:
        res = await self._request('eth_sendRawTransaction', args)
        return res
