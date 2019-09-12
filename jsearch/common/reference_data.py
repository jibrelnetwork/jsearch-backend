import asyncio
from typing import NamedTuple
import json

import aiohttp

from jsearch import settings


class BlockReferenceData(NamedTuple):
    hash: str
    number: int


class ReferenceDataProvider:
    name = None

    def __init__(self, root_url: str, api_key: str):
        self.root_url = root_url
        self.api_key = api_key

    async def get_last_block(self):
        raise NotImplementedError


class Web3ApiProvider(ReferenceDataProvider):

    def get_base_url(self):
        return self.root_url

    async def get_last_block(self) -> BlockReferenceData:
        payload = {'jsonrpc': '2.0',
                   'method': 'eth_getBlockByNumber',
                   'params': ['latest', False],
                   'id': 1}
        session = aiohttp.ClientSession()
        async with session.post(self.get_base_url(), json=payload) as resp:
            resp_text = await resp.text()
            data = json.loads(resp_text)['result']
        return BlockReferenceData(hash=data['hash'], number=int(data['number'], 16))


class EtherscanDataProvider(ReferenceDataProvider):
    name = 'etherscan'

    async def get_last_block(self):
        query_params = {
            'module': 'proxy',
            'action': 'eth_getBlockByNumber',
            'tag': 'latest',
            'boolean': 'false',
            'apikey': self.api_key
        }
        session = aiohttp.ClientSession()
        async with session.get(self.root_url, params=query_params) as resp:
            resp_text = await resp.text()
            data = json.loads(resp_text)['result']
        await session.close()
        return BlockReferenceData(hash=data['hash'], number=int(data['number'], 16))


class InfuraDataProvider(Web3ApiProvider):
    name = 'infura'

    def get_base_url(self):
        return '{}/{}'.format(self.root_url, self.api_key)


class JwalletDataProvider(Web3ApiProvider):
    name = 'jwallet'


async def get_ref_blocks():
    refs = [
        EtherscanDataProvider(settings.ETHERSCAN_API_URL, settings.ETHERSCAN_API_KEY),
        InfuraDataProvider(settings.INFURA_API_URL, settings.INFURA_API_KEY),
        JwalletDataProvider(settings.JWALLET_API_URL, None),
    ]
    coros = [r.get_last_block() for r in refs]
    blocks = await asyncio.gather(*coros)
    return {r.name: b.number for r, b in zip(refs, blocks)}
