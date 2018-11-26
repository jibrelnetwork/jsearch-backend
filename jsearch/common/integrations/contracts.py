import logging
from typing import Dict, Any

import requests

from jsearch.settings import JSEARCH_CONTRACTS_API

logger = logging.getLogger(__name__)


def get_contract(address: str):
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


def patch_contract(address: str, data: Dict[str, Any]):
    url = f'{JSEARCH_CONTRACTS_API}/v1/contracts/{address}'
    resp = requests.patch(url, json=data)
    if resp.status_code == 204:
        logger.info('Token info updated for address %s', address)
    else:
        logger.error('Token info update error for address %s: %s', address, resp.status_code)
