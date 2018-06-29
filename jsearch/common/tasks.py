import os

from jsearch.common.celery import app
from jsearch.common.contracts import ERC20_ABI, wait_install_solc
from jsearch.common.database import get_main_db
from jsearch import settings

from web3 import Web3


def update_token_info(address):
    w3 = Web3(Web3.HTTPProvider(settings.ETH_NODE_URL))
    checksum_address = Web3.toChecksumAddress(address)
    c = w3.eth.contract(checksum_address, abi=ERC20_ABI)
    info = {
        'token_name': c.functions.name().call(),
        'token_symbol': c.functions.symbol().call(),
        'token_decimals': c.functions.decimals().call(),
        'token_total_supply': c.functions.totalSupply().call(),
    }
    db = get_main_db()
    db.call_sync(db.update_contract(address, info))


@app.task
def process_new_verified_contract_transactions(address):
    update_token_info(address)
    db = get_main_db()
    c = db.call_sync(db.get_contract(address))
    for tx in db.get_contract_transactions(address):
        process_token_transfer.delay(c, tx)


@app.task
def process_token_transfer(contract, tx):
    db.process_token_transfers(tx['hash'])


@app.task
def install_solc(commit):
    wait_install_solc(commit)