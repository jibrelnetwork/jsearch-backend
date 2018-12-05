import logging

from web3 import Web3

from jsearch import settings
from jsearch.common.contracts import ERC20_ABI
from jsearch.common.integrations.contracts import patch_contract
from jsearch.utils import suppress_exception

logger = logging.getLogger(__name__)


@suppress_exception
def update_token_info(address, abi=None):
    abi = abi or ERC20_ABI
    w3 = Web3(Web3.HTTPProvider(settings.ETH_NODE_URL))
    checksum_address = Web3.toChecksumAddress(address)
    c = w3.eth.contract(checksum_address, abi=abi)
    info = {
        'token_name': c.functions.name().call(),
        'token_symbol': c.functions.symbol().call(),
        'token_decimals': c.functions.decimals().call(),
        'token_total_supply': c.functions.totalSupply().call(),
    }

    if isinstance(info['token_name'], bytes):
        info['token_name'] = info['token_name'].decode().replace('\x00', '')
    if isinstance(info['token_symbol'], bytes):
        info['token_symbol'] = info['token_symbol'].decode().replace('\x00', '')

    patch_contract(address, info)

    logger.info('Token info updated for token %s name %s', address, info['token_name'])
