import asyncio
import logging

import aiohttp

from jsearch import settings
from jsearch.api.error_code import ErrorCode
from jsearch.api.helpers import (
    get_tag,
    validate_params,
    api_success,
    proxy_response,
    api_error,
    api_error_400,
    api_error_404
)
from jsearch.common import tasks
from jsearch.common.contracts import cut_contract_metadata_hash
from jsearch.common.contracts import is_erc20_compatible

log = logging.getLogger(__name__)


async def get_account(request):
    """
    Get account by adress
    """
    storage = request.app['storage']
    address = request.match_info.get('address').lower()
    tag = get_tag(request)

    account = await storage.get_account(address, tag)
    if account is None:
        return api_error_404()
    return api_success(account.to_dict())


async def get_account_transactions(request):
    """
    Get account transactions
    """
    storage = request.app['storage']
    address = request.match_info.get('address').lower()
    params = validate_params(request, default_order='asc')

    txs = await storage.get_account_transactions(address, params['limit'], params['offset'], params['order'])
    return api_success([t.to_dict() for t in txs])


async def get_account_internal_transactions(request):
    """
    Get account internal transactions
    """
    # todo: implement it
    return api_success([])


async def get_account_pending_transactions(request):
    """
    Get account pending transactions
    """
    # todo: implement it
    return api_success([])


async def get_account_logs(request):
    """
    Get contract logs
    """
    # todo: implement it
    return api_success([])


async def get_account_mined_blocks(request):
    """
    Get account mined blocks
    """
    storage = request.app['storage']
    address = request.match_info.get('address').lower()
    params = validate_params(request)

    blocks = await storage.get_account_mined_blocks(address, params['limit'], params['offset'], params['order'])
    return api_success([b.to_dict() for b in blocks])


async def get_account_mined_uncles(request):
    """
    Get account mined uncles
    """
    storage = request.app['storage']
    address = request.match_info.get('address').lower()
    params = validate_params(request)

    uncles = await storage.get_account_mined_uncles(address, params['limit'], params['offset'], params['order'])
    return api_success([u.to_dict() for u in uncles])


async def get_accounts_balances(request):
    """
    Get ballances for list of accounts
    """
    storage = request.app['storage']
    addresses = request.query.get('addresses', '').lower().split(',')

    if len(addresses) > settings.API_QUERY_ARRAY_MAX_LENGTH:
        return api_error_400(errors=[
            {
                'field': 'addresses',
                'error_code': ErrorCode.TOO_MANY_ITEMS,
                'error_message': 'Too many addresses requested'
            }
        ])

    balances = await storage.get_accounts_balances(addresses)
    return api_success([b.to_dict() for b in balances])


async def get_blocks(request):
    """
    Get blocks list
    """
    params = validate_params(request)

    storage = request.app['storage']
    blocks = await storage.get_blocks(params['limit'], params['offset'], params['order'])
    return api_success([block.to_dict() for block in blocks])


async def get_block(request):
    """
    Get block by hash or number
    """
    storage = request.app['storage']
    tag = get_tag(request)
    block = await storage.get_block(tag)
    if block is None:
        return api_error_404()
    return api_success(block.to_dict())


async def get_block_transactions(request):
    storage = request.app['storage']
    tag = get_tag(request)
    txs = await storage.get_block_transactions(tag)
    return api_success([t.to_dict() for t in txs])


async def get_block_uncles(request):
    storage = request.app['storage']
    tag = get_tag(request)
    uncles = await storage.get_block_uncles(tag)
    return api_success([u.to_dict() for u in uncles])


async def get_transaction(request):
    storage = request.app['storage']
    txhash = request.match_info.get('txhash')

    transaction = await storage.get_transaction(txhash)
    if transaction is None:
        return api_error_404()
    return api_success(transaction.to_dict())


async def get_internal_transactions(request):
    """
    Get internal transactions by transaction hash
    """
    # todo: implement it
    return api_success([])


async def get_pending_transactions(request):
    """
    Get pending transactions by transaction hash
    """
    # todo: implement it
    return api_success([])


async def get_receipt(request):
    storage = request.app['storage']
    txhash = request.match_info.get('txhash')

    receipt = await storage.get_receipt(txhash)
    if receipt is None:
        return api_error_404()
    return api_success(receipt.to_dict())


async def get_uncles(request):
    """
    Get uncles list
    """
    params = validate_params(request)
    storage = request.app['storage']
    uncles = await storage.get_uncles(params['limit'], params['offset'], params['order'])
    return api_success([uncle.to_dict() for uncle in uncles])


async def get_uncle(request):
    """
    Get uncle by hash or number
    """
    storage = request.app['storage']
    tag = get_tag(request)
    uncle = await storage.get_uncle(tag)
    if uncle is None:
        return api_error_404()
    return api_success(uncle.to_dict())


async def verify_contract(request):
    """
    address
    contract_name
    compiler
    optimization_enabled
    constructor_args
    source_code
    """

    input_data = await request.json()
    constructor_args = input_data.pop('constructor_args') or ''
    address = input_data.pop('address')

    contract_creation_code = await request.app['storage'].get_contact_creation_code(address)

    async with aiohttp.request('POST', settings.JSEARCH_COMPILER_API + '/v1/compile', json=input_data) as resp:
        res = await resp.json()

    byte_code = res['bin']
    byte_code, _ = cut_contract_metadata_hash(byte_code)
    bc_byte_code, mhash = cut_contract_metadata_hash(contract_creation_code)

    if byte_code + constructor_args == bc_byte_code.replace('0x', ''):
        verification_passed = True
        if is_erc20_compatible(res['abi']):
            is_erc20_token = True
        else:
            is_erc20_token = False
        contract_data = dict(
            address=address,
            contract_creation_code=contract_creation_code,
            mhash=mhash,
            abi=res['abi'],
            constructor_args=constructor_args,
            is_erc20_token=is_erc20_token,
            **input_data
        )
        async with aiohttp.request('POST', settings.JSEARCH_CONTRACTS_API + '/v1/contracts',
                                   json=contract_data) as resp:
            res = await resp.json()
    else:
        verification_passed = False
    return api_success({'verification_passed': verification_passed})


async def get_token_transfers(request):
    storage = request.app['storage']
    params = validate_params(request)
    contract_address = request.match_info['address'].lower()

    transfers = await storage.get_tokens_transfers(
        address=contract_address,
        limit=params['limit'],
        offset=params['offset'],
        order=params['order']
    )
    return api_success([transfer.to_dict() for transfer in transfers])


async def get_account_token_transfers(request):
    # todo: need to add validation. I'm worried about max size of limit
    storage = request.app['storage']
    params = validate_params(request)
    account_address = request.match_info['address'].lower()

    transfers = await storage.get_account_tokens_transfers(
        address=account_address,
        limit=params['limit'],
        offset=params['offset'],
        order=params['order']
    )
    return api_success([transfer.to_dict() for transfer in transfers])


async def get_token_holders(request):
    storage = request.app['storage']
    params = validate_params(request)
    token_address = request.match_info['address'].lower()

    holders = await storage.get_tokens_holders(
        address=token_address,
        limit=params['limit'],
        offset=params['offset'],
        order=params['order']
    )
    return api_success([holder.to_dict() for holder in holders])


async def get_account_token_balance(request):
    storage = request.app['storage']
    token_address = request.match_info['token_address'].lower()
    account_address = request.match_info['address'].lower()

    holder = await storage.get_account_token_balance(
        account_address=account_address,
        token_address=token_address,
    )
    if holder is None:
        return api_error_404()
    return api_success(holder.to_dict())


async def get_gas_price(request):
    resp = await request.app['node_proxy'].gas_price()
    return proxy_response(resp)


async def get_transaction_count(request):
    args = await request.json()
    resp = await request.app['node_proxy'].transaction_count(args)
    return proxy_response(resp)


async def calculate_estimate_gas(request):
    args = await request.json()
    resp = await request.app['node_proxy'].estimate_gas(args)
    return proxy_response(resp)


async def call_contract(request):
    args = await request.json()
    resp = await request.app['node_proxy'].call_contract(args)
    return proxy_response(resp)


async def send_raw_transaction(request):
    args = await request.json()
    resp = await request.app['node_proxy'].send_raw_transaction(args)
    return proxy_response(resp)


async def on_new_contracts_added(request):
    data = await request.json()
    address = data['address']
    address = address and address.lower()
    if settings.ENABLE_RESET_POST_PROCESSING:
        tasks.on_new_contracts_added_task.delay(address)
    return api_success({})


async def get_blockchain_tip(request):
    tip = request.query.get('tip')
    storage = request.app['storage']
    block_status = await storage.get_blockchain_tip_status(tip)
    if block_status is None:
        err = {
            'field': 'tip',
            'error_code': ErrorCode.BLOCK_NOT_FOUND,
            'error_message': f'Block with hash {tip} not found'
        }
        return api_error(status=404, errors=[err])
    return api_success(block_status)


async def get_assets_summary(request):
    params = validate_params(request)
    addresses = request.query.get('addresses', '')
    addresses = addresses.split(',') if addresses else []
    assets = request.query.get('assets', '')
    assets = [a for a in assets.split(',') if a]
    storage = request.app['storage']
    summary = await storage.get_wallet_assets_summary(
        addresses,
        limit=params['limit'],
        offset=params['offset'],
        assets=assets)
    return api_success(summary)


async def get_wallet_transfers(request):
    params = validate_params(request)
    addresses = request.query.get('addresses', '')
    addresses = addresses.split(',') if addresses else []
    assets = request.query.get('assets', '')
    assets = [a for a in assets.split(',') if a]
    storage = request.app['storage']
    transfers = await storage.get_wallet_assets_transfers(
        addresses,
        limit=params['limit'],
        offset=params['offset'],
        assets=assets)
    return api_success([t.to_dict() for t in transfers])


async def get_wallet_transactions(request):
    params = validate_params(request)
    address = request.query.get('address', '')
    storage = request.app['storage']
    txs_task = storage.get_wallet_transactions(
        address,
        limit=params['limit'],
        offset=params['offset'])
    nonce_task = storage.get_nonce(address)
    results = await asyncio.gather(txs_task, nonce_task)
    txs = [t.to_dict() for t in results[0]]
    nonce = results[1]
    result = {
        'transactions': txs,
        'pendingTransactions': [],
        'outgoingTransactionsNumber': nonce
    }
    return api_success(result)
