import os
import logging
import urllib.request

from web3 import Web3
from lxml import html
import solc.install
from celery.signals import celeryd_init

from jsearch.common.celery import app
from jsearch.common.contracts import ERC20_ABI, wait_install_solc
from jsearch import settings


logger = logging.getLogger(__name__)


def update_token_info(address, db=None):
    from jsearch.common.database import get_main_db
    w3 = Web3(Web3.HTTPProvider(settings.ETH_NODE_URL))
    checksum_address = Web3.toChecksumAddress(address)
    c = w3.eth.contract(checksum_address, abi=ERC20_ABI)
    info = {
        'token_name': c.functions.name().call(),
        'token_symbol': c.functions.symbol().call(),
        'token_decimals': c.functions.decimals().call(),
        'token_total_supply': c.functions.totalSupply().call(),
    }
    if db is None:
        db = get_main_db()
    db.call_sync(db.update_contract(address, info))
    logger.info('Token info updated for address %s', address)


@app.task
def process_new_verified_contract_transactions(address):
    from jsearch.common.database import get_main_db
    logger.info('Starting process_new_verified_contract_transactions for address %s', address)
    db = get_main_db()
    c = db.call_sync(db.get_contract(address))
    if c is None:
        logger.info('Contract %s is missed in DB', address)
        return
    try:
        update_token_info(address, db)
        tx_count = 0
        for tx in db.call_sync(db.get_contract_transactions(address)):
            process_token_transfer.delay(tx)
            tx_count += 1
        logger.info('%s transactions found for %s', tx_count, address)
    finally:
        db.call_sync(db.disconnect())


@app.task
def process_token_transfer(tx):
    from jsearch.common.database import get_main_db
    logger.info('Starting process_token_transfer for tx %s', tx['hash'])
    db = get_main_db()
    db.process_token_transfers(tx['hash'])


@app.task
def update_token_holder_balance_task(token_address, account_address, block_number):
    from jsearch.common.database import get_engine, update_token_holder_balance
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
    db = get_engine()
    update_token_holder_balance(db, token_address, account_address, balance)
    logger.info('Token balance updated for token %s account %s block %s value %s', token_address, account_address, block_number, balance)
