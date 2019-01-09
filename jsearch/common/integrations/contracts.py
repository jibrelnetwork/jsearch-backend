import logging

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
    # Todo: think about retries.

    Args:
        address: contract address in blockchain
    """
    response = requests.get(url=f"{JSEARCH_CONTRACTS_API}/v1/contracts/{address}")
    if response.status_code == 200:
        logger.debug('Got Contract %s: %s', address, response.status_code)
        return response.json()

    logger.debug('Miss Contract %s: %s', address, response.status_code)
