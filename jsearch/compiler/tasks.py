import os

from jsearch.compiler.utils import compile_contract, get_contract
from jsearch.compiler.celery import app

from jsearch.common.contract_utils import ERC20_ABI
from jsearch.syncer.database import get_main_db

from web3 import Web3


DEFAULT_OPTIMIZER_RUNS = 200


@app.task
def compile_contract_task(source_code, contract_name, compiler, optimization_enabled):
    compile_result = compile_contract(
        source_code,
        contract_name,
        compiler,
        optimization_enabled,
        DEFAULT_OPTIMIZER_RUNS)
    return compile_result


@app.task
def write_token_info(address):
    w3 = Web3(Web3.HTTPProvider(os.environ['ETH_NODE_URL']))
    checksum_address = Web3.toChecksumAddress(address)
    c = w3.eth.contract(checksum_address, abi=ERC20_ABI)
    info = {
        'token_name': c.functions.name().call(),
        'token_symbol': c.functions.symbol().call(),
        'token_decimals': c.functions.decimals().call(),
        'token_total_supply': c.functions.totalSupply().call(),
    }

    db = get_main_db()
    db.update_contract(address, info)

