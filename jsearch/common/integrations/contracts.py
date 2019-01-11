import logging
from typing import Dict, List, Set, Union

import backoff
import requests
from cachetools import TTLCache, cached

from jsearch.settings import JSEARCH_CONTRACTS_API
from jsearch.typing import Contract

logger = logging.getLogger(__name__)


@cached(cache=TTLCache(maxsize=1000, ttl=60 * 5))
@backoff.on_exception(backoff.fibo, max_tries=10, exception=requests.RequestException)
def get_contract(address: str) -> Contract:
    """
    Get contract from internal service

    Args:
        address: contract address in blockchain
    """
    response = requests.get(url=f"{JSEARCH_CONTRACTS_API}/v1/contracts/{address}")
    if response.status_code == 200:
        logger.debug('Got Contract %s: %s', address, response.status_code)
        return response.json()

    logger.debug('Miss Contract %s: %s', address, response.status_code)


@backoff.on_exception(backoff.fibo, max_tries=10, exception=requests.RequestException)
def get_contracts(addresses: Union[List[str], Set[str]]) -> Dict[str, Contract]:
    addresses_str = ','.join(addresses)
    url = f"{JSEARCH_CONTRACTS_API}/v1/contracts/getmany?addresses={addresses_str}"

    response = requests.get(url)
    if response.status_code == 200:
        logger.debug('Got Contract %s: count %s', response.status_code, len(addresses))
        data = response.json()
        return {contract['address']: contract for contract in data}

    logger.debug('Miss Contract %s: count %s', response.status_code, len(addresses))
