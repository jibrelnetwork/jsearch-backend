import asyncio
import logging

from jsearch.api.error_code import ErrorCode
from jsearch.api.helpers import (
    get_tag,
    validate_params,
    api_success,
    api_error,
    api_error_404,
    get_from_joined_string,
)

logger = logging.getLogger(__name__)


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
        assets=assets
    )
    return api_success([t.to_dict() for t in transfers])


async def get_wallet_transactions(request):
    params = validate_params(request)
    address = request.query.get('address', '')
    storage = request.app['storage']
    txs_task = storage.get_wallet_transactions(
        address,
        limit=params['limit'],
        offset=params['offset']
    )
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


async def get_transaction(request):
    storage = request.app['storage']
    txhash = request.match_info.get('txhash')

    transaction = await storage.get_transaction(txhash)
    if transaction is None:
        return api_error_404()
    return api_success(transaction.to_dict())
