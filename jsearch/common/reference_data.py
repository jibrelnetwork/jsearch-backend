import logging
import asyncio
import json
from typing import NamedTuple, Optional

import aiohttp

from jsearch import settings

logger = logging.getLogger(__name__)


class BlockReferenceData(NamedTuple):
    hash: str
    number: int


class ReferenceDataProvider:
    # FIXME (nickgashkov): Set to `NotImplemented` or `...` by default.
    name: str = None  # type: ignore

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
        async with aiohttp.ClientSession() as session:
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
        async with aiohttp.ClientSession() as session:
            async with session.get(self.root_url, params=query_params) as resp:
                resp_text = await resp.text()
                data = json.loads(resp_text)['result']
        return BlockReferenceData(hash=data['hash'], number=int(data['number'], 16))


class InfuraDataProvider(Web3ApiProvider):
    name = 'infura'

    def get_base_url(self):
        return '{}/{}'.format(self.root_url, self.api_key)


class JwalletDataProvider(Web3ApiProvider):
    name = 'jwallet'


lag_statistics = {
    EtherscanDataProvider.name: 0,
    InfuraDataProvider.name: 0,
    JwalletDataProvider.name: 0,
}


async def get_last_block_safe(data_provider: ReferenceDataProvider) -> Optional[BlockReferenceData]:
    try:
        return await data_provider.get_last_block()
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.info('[LAG] Failed to fetch last block for %s', data_provider.name, exc_info=True)

    return None


async def set_lag_statistics(latest_synced_block_number):
    refs = [
        EtherscanDataProvider(settings.ETHERSCAN_API_URL, settings.ETHERSCAN_API_KEY),
        InfuraDataProvider(settings.INFURA_API_URL, settings.INFURA_API_KEY),
        JwalletDataProvider(settings.JWALLET_API_URL, None),
    ]
    coros = [get_last_block_safe(r) for r in refs]
    blocks = await asyncio.gather(*coros)

    for ref, ref_block in zip(refs, blocks):
        if ref_block is None:
            # Keep the old reference if the data provider failed to obtain the
            # block.
            continue

        lag_statistics[ref.name] = ref_block.number - latest_synced_block_number


def get_lag_statistics():
    return lag_statistics


def get_lag_statistics_by_provider(name):
    return lag_statistics[name]
