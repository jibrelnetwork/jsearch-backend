import asyncio
import logging

import aiohttp
from aiohttp import web
from functools import partial
from typing import Tuple, Optional, Union, Dict

from jsearch import settings
from jsearch.api.error_code import ErrorCode
from jsearch.api.helpers import ApiError, ORDER_ASC
from jsearch.api.helpers import (
    get_tag,
    validate_params,
    api_success,
    proxy_response,
    api_error,
    api_error_400,
    api_error_404,
    get_from_joined_string,
)
from jsearch.api.structs import BlockInfo
from jsearch.common import tasks, stats
from jsearch.common.contracts import cut_contract_metadata_hash, is_erc20_compatible

logger = logging.getLogger(__name__)


async def get_account(request):
    """
    Get account by address
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
    storage = request.app['storage']
    address = request.match_info.get('address').lower()
    params = validate_params(request)

    internal_txs = await storage.get_account_internal_transactions(
        address,
        limit=params['limit'],
        offset=params['offset'],
        order=params['order'],
    )

    response_data = [it.to_dict() for it in internal_txs]
    response = api_success(response_data)

    return response


async def get_account_pending_transactions(request):
    """
    Get account pending transactions
    """
    storage = request.app['storage']
    address = request.match_info.get('address').lower()
    params = validate_params(request)

    pending_txs = await storage.get_account_pending_transactions(
        address,
        order=params['order'],
        limit=params['limit'],
        offset=params['offset'],
    )

    response_data = [pt.to_dict() for pt in pending_txs]
    response = api_success(response_data)

    return response


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
    addresses = get_from_joined_string(request.query.get('addresses'))

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
    storage = request.app['storage']
    tx_hash = request.match_info.get('txhash').lower()
    params = validate_params(request)

    internal_txs = await storage.get_internal_transactions(
        tx_hash,
        limit=params['limit'],
        offset=params['offset'],
        order=params['order'],
    )

    response_data = [it.to_dict() for it in internal_txs]
    response = api_success(response_data)

    return response


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
    storage = request.app['storage']

    block = await storage.get_latest_block_info()
    if block is None:
        err = {
            'error_code': ErrorCode.BLOCK_NOT_FOUND,
            'error_message': f'Blockchain tip not found'
        }
        return api_error(status=404, errors=[err])

    return api_success({
        'blockHash': block.hash,
        'blockNumber': block.number
    })


async def get_assets_summary(request):
    params = validate_params(request)
    addresses = get_from_joined_string(request.query.get('addresses'))
    assets = get_from_joined_string(request.query.get('assets'))
    storage = request.app['storage']
    summary = await storage.get_wallet_assets_summary(
        addresses,
        limit=params['limit'],
        offset=params['offset'],
        assets=assets)
    return api_success(summary)


async def get_wallet_transfers(request):
    params = validate_params(request)
    addresses = get_from_joined_string(request.query.get('addresses'))
    assets = get_from_joined_string(request.query.get('assets'))
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


MAX_COUNT = 1000
MAX_LIMIT = 1000
MAX_OFFSET = 10000


def get_address(request) -> str:
    address = request.query.get('blockchain_address', '').lower()
    if not address:
        raise ApiError(
            {
                'param': 'blockchain_address',
                'error_code': ErrorCode.PARAM_REQUIRED,
                'error_message': f'Query param `blockchain_address` is required'
            },
            status=400
        )
    return address


def get_tip_hash(request) -> Optional[str]:
    tip = request.query.get('blockchain_tip')
    if tip is None:
        raise ApiError(
            {
                'param': 'blockchain_tip',
                'error_code': ErrorCode.PARAM_REQUIRED,
                'error_message': f'Query param `blockchain_tip` is required'
            },
            status=400
        )

    return tip


async def get_tip_block(storage, block_hash: str) -> BlockInfo:
    block_info = await storage.get_block_number(block_hash)
    if block_info is None:
        raise ApiError(
            {
                'field': 'tip',
                'error_code': ErrorCode.BLOCK_NOT_FOUND,
                'error_message': f'Block with hash {block_hash} not found'
            },
            status=404
        )
    return block_info


def get_positive_number(request: aiohttp.RequestInfo,
                        attr: str,
                        tags=Optional[Dict[str, int]],
                        is_required=False) -> Optional[Union[int, str]]:
    value = request.query.get(attr, "").lower()

    if value.isdigit():
        number = int(value)
        if number >= 0:
            return number

    if value and tags and value in tags:
        return tags[value]

    if not value and not is_required:
        return None

    msg_allowed_tags = tags and f"or tag ( {', '.join(tags.keys())} )" or " "
    if is_required:
        raise ApiError(
            {
                'field': attr,
                'error_code': ErrorCode.PARAM_REQUIRED,
                'error_message': f'Query param `{attr}` is required'
            },
            status=400
        )

    raise ApiError(
        {
            'field': attr,
            'error_code': ErrorCode.UNKNOWN_VALUE,
            'error_message': f'Parameter `{attr}` must be positive integer {msg_allowed_tags} or empty'
        },
        status=400
    )


def get_block_range(request: aiohttp.RequestInfo,
                    tip_block: BlockInfo,
                    latest_block: BlockInfo,
                    is_asc_order: bool) -> Tuple[int, int]:
    get_block_number = partial(get_positive_number, tags={"latest": latest_block.number, "tip": tip_block.number})

    count = get_positive_number(request, 'block_range_count')
    start_from = get_block_number(request, 'block_range_start', is_required=True)
    until_to = get_block_number(request, 'block_range_end')

    # set default value only if count is None
    if count is None and until_to is None:
        until_to = tip_block.number

    if count is not None:

        if count < 0:
            count = 0

        if count is not None and count > 0:
            count -= 1

        if until_to is None:
            offset = count if is_asc_order else count * -1
            until_to = start_from + offset

        is_reversed = start_from > until_to if is_asc_order else start_from < until_to
        if is_reversed:
            start_from, until_to = until_to, start_from

        if is_asc_order:
            until_to = min(start_from + count, until_to)
        else:
            until_to = max(start_from - count, until_to)

    return start_from, until_to


@ApiError.catch
async def get_wallet_events(request):
    storage = request.app['storage']
    params = validate_params(request, max_limit=MAX_LIMIT, max_offset=MAX_OFFSET, default_order='asc')

    address = get_address(request)
    tip_hash = get_tip_hash(request)

    tip_block = await get_tip_block(storage, block_hash=tip_hash)
    latest_block = await storage.get_latest_block_info()

    start_from, until_to = get_block_range(request, tip_block, latest_block, is_asc_order=params['order'] == ORDER_ASC)

    get_events_task = storage.get_wallet_events(address, start_from, until_to, **params)
    get_txs_task = storage.get_wallet_events_transactions(address, start_from, until_to, **params)

    wallet_events, txs = await asyncio.gather(get_events_task, get_txs_task)

    tip = await storage.get_blockchain_tip(tip=tip_block, last_block=latest_block)
    is_event_affected = (
            tip.is_in_fork and
            tip.last_unchanged_block is not None and until_to > tip.last_unchanged_block
    )

    if is_event_affected:
        events = []
    else:
        events = [{'rootTxData': tx, 'events': wallet_events.get(tx['hash'])} for tx in txs]

    pending_events = []
    include_pending_events = request.query.get('include_pending_events', False)
    if include_pending_events:
        pending_events = await storage.get_account_pending_events(address, order=ORDER_ASC)

    return api_success({
        "blockchainTip": tip.to_dict(),
        "events": events,
        "pending_events": pending_events
    })


async def healthcheck(request: web.Request) -> web.Response:
    main_db_stats = await stats.get_db_stats(request.app['db_pool'])
    node_stats = await stats.get_node_stats(request.app['node_proxy'])
    loop_stats = await stats.get_loop_stats()

    healthy = all(
        (
            main_db_stats.is_healthy,
            node_stats.is_healthy,
            loop_stats.is_healthy,
        )
    )
    status = 200 if healthy else 400

    data = {
        'healthy': healthy,
        'isMainDbHealthy': main_db_stats.is_healthy,
        'isNodeHealthy': node_stats.is_healthy,
        'isLoopHealthy': loop_stats.is_healthy,
        'loopTasksCount': loop_stats.tasks_count,
    }

    return web.json_response(data=data, status=status)
