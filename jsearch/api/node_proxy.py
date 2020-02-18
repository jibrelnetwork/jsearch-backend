import asyncio
from jsearch import settings
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
        if isinstance(node_url, str):
            self.node_urls = [node_url, ]
        elif isinstance(node_url, list):
            self.node_urls = node_url

    async def _fetch(self, session, url, message, headers):
        async with session.post(url, json=message, headers=headers) as resp:
            return await resp.json()

    @backoff.on_exception(backoff.expo, aiohttp.ClientError, max_tries=MAX_RETRY)
    async def _request(self, method: str, params: List = None):
        params = params or []
        message = {'jsonrpc': '2.0', 'method': method, 'params': params, 'id': 1}
        headers = {'User-Agent': settings.HTTP_USER_AGENT}

        async with aiohttp.ClientSession() as session:
            tasks = [self._fetch(session, url, message, headers) for url in self.node_urls]

            return await asyncio.gather(*tasks)

    async def client_version(self) -> str:
        res = await self._request('web3_clientVersion')
        # we need to get a response from the first node only
        return res[0]

    async def gas_price(self) -> str:
        res = await self._request('eth_gasPrice')
        # we need to get a response from the first node only
        res = _decimal_result(res[0])
        return res

    async def call_contract(self, args: List) -> Any:
        res = await self._request('eth_call', args)
        # we need to get a response from the first node only
        return res[0]

    async def estimate_gas(self, args: List) -> str:
        res = await self._request('eth_estimateGas', args)
        # we need to get a response from the first node only
        res = _decimal_result(res[0])
        return res

    async def send_raw_transaction(self, args: List) -> str:
        res = await self._request('eth_sendRawTransaction', args)

        # we must find and return a first success response
        # otherwise return a first received response
        for node_response in res:
            if "error" not in node_response:
                return node_response

        return res[0]

    async def transaction_count(self, args: List) -> str:
        res = await self._request('eth_getTransactionCount', args)
        # we need to get a response from the first node only
        res = _decimal_result(res[0])
        return res

    async def get_proof(self, args: List) -> str:
        res = await self._request('eth_getProof', args)
        # we need to get a response from the first node only
        return res[0]
