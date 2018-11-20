import logging

import requests

from jsearch.settings import JSEARCH_CONTRACTS_API

logger = logging.getLogger(__name__)


def get_contract(address):
    """
    Get contract from internal service
    # Todo: think about retries.

    Args:
        address: contract address in blockchain
    """
    response = requests.get(url=f"{JSEARCH_CONTRACTS_API}/v1/contracts/address")
    if response.status_code == 200:
        logger.debug('Got Contract %s: %s', address, response.status_code)
        return response.json()

    logger.debug('Miss Contract %s: %s', address, response.status_code)
