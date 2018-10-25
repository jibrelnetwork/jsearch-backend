import os
import logging

from web3 import Web3
import requests

from jsearch.common.celery import app
from jsearch.common.contracts import ERC20_ABI
from jsearch import settings


logger = logging.getLogger(__name__)


def update_token_info(address, abi=None):
    from jsearch.common.database import get_main_db
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
    url = '{}/v1/contracts/{}'.format(settings.JSEARCH_CONTRACTS_API, address)
    resp = requests.patch(url, json=info)
    if resp.status_code == 204:
        logger.info('Token info updated for address %s', address)
    else:
        logger.error('Token info update error for address %s: %s', address, resp.status_code)


# @app.task
# def process_new_verified_contract_transactions(address):
#     from jsearch.common.database import get_main_db
#     logger.info('Starting process_new_verified_contract_transactions for address %s', address)
#     db = get_main_db()
#     c = db.call_sync(db.get_contract(address))
#     if c is None:
#         logger.info('Contract %s is missed in DB', address)
#         return
#     try:
#         update_token_info(address, db)
#         tx_count = 0
#         for tx in db.call_sync(db.get_contract_transactions(address)):
#             process_token_transfer.delay(tx)
#             tx_count += 1
#         logger.info('%s transactions found for %s', tx_count, address)
#     finally:
#         db.call_sync(db.disconnect())


@app.task
def update_token_holder_balance_task(token_address, account_address, block_number):
    from jsearch.common.database import get_main_db
    logger.info('Updating Token balance for token %s account %s block %s', token_address, account_address, block_number)
    w3 = Web3(Web3.HTTPProvider(settings.ETH_NODE_URL))
    checksum_token_address = Web3.toChecksumAddress(token_address)
    checksum_account_address = Web3.toChecksumAddress(account_address)
    c = w3.eth.contract(checksum_token_address, abi=ERC20_ABI)
    # balance = c.functions.balanceOf(checksum_account_address).call(block_identifier=block_number)
    balance = c.functions.balanceOf(checksum_account_address).call()
    # decimals = c.functions.decimals().call(block_identifier=block_number)
    decimals = c.functions.decimals().call()
    balance = balance / 10 ** decimals
    db = get_main_db()
    db.update_token_holder_balance(token_address, account_address, balance)
    logger.info('Token balance updated for token %s account %s block %s value %s', token_address, account_address, block_number, balance)


@app.task
def on_new_contracts_added_task(address, abi):
    from jsearch.common.database import get_main_db
    logger.info('Starting process transactions for contract at %s', address)
    update_token_info(address, abi)
    db = get_main_db()
    tx_count = 0
    for tx in db.get_contract_transactions(address):
        db.process_token_transfers(tx['hash'])
        tx_count += 1
    logger.info('%s transactions found for %s', tx_count, address)
