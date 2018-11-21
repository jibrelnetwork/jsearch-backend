import logging
import time
from concurrent.futures import as_completed
from concurrent.futures.thread import ThreadPoolExecutor
from functools import partial
from typing import Tuple, List, Dict

from web3 import Web3

from jsearch import settings
from jsearch.common.contracts import ERC20_ABI
from jsearch.common.integrations.contracts import patch_contract

logger = logging.getLogger(__name__)

OPERATION_UPDATE_CONTRACT_INFO = 'update_contract_info'
OPERATION_UPDATE_TOKEN_BALANCE = 'update_token_balance'


def get_balance(token_address, account_address):
    w3 = Web3(Web3.HTTPProvider(settings.ETH_NODE_URL))

    checksum_token_address = Web3.toChecksumAddress(token_address)
    checksum_account_address = Web3.toChecksumAddress(account_address)

    c = w3.eth.contract(checksum_token_address, abi=ERC20_ABI)
    # balance = c.functions.balanceOf(checksum_account_address).call(block_identifier=block_number)

    balance = c.functions.balanceOf(checksum_account_address).call()
    # decimals = c.functions.decimals().call(block_identifier=block_number)

    decimals = c.functions.decimals().call()
    return balance / 10 ** decimals


def update_token_holder_balance(db, token_address, account_address, block_number=None):
    logging.info('update_token_balance_info')

    balance = get_balance(token_address, account_address)
    db.update_token_holder_balance(token_address, account_address, balance)

    logger.info(
        'Token balance updated for token %s account %s block %s value %s',
        token_address, account_address, block_number, balance
    )


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


def do_operations_bulk(db, operations: Dict[str, List[Tuple[str, str]]]):
    start_time = time.monotonic()

    futures = []
    with ThreadPoolExecutor(settings.JSEARCH_POST_PROCESSING_THREADS) as executor:
        for args_list in operations[OPERATION_UPDATE_CONTRACT_INFO]:
            func = partial(update_token_info, *args_list)
            future = executor.submit(func)
            futures.append(future)

        for args in operations[OPERATION_UPDATE_TOKEN_BALANCE]:
            func = partial(update_token_holder_balance, db, *args)
            future = executor.submit(func)
            futures.append(future)

        for futures in as_completed(futures):
            futures.result()

    update_balance_operations = (operations[OPERATION_UPDATE_TOKEN_BALANCE])
    update_token_info_operations = len(operations[OPERATION_UPDATE_CONTRACT_INFO])

    end_time = start_time - time.monotonic()
    logger.info("%s update token info on %0.2fs", update_balance_operations, end_time)
    logger.info("%s update token holder balance on %0.2fs", update_token_info_operations, end_time)
