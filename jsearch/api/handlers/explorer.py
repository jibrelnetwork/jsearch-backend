import logging

from jsearch.api.blockchain_tip import maybe_apply_tip
from jsearch.api.helpers import (
    get_tag,
    validate_params,
    api_success,
    api_error_response_404,
    ApiError)

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
        return api_error_response_404()
    return api_success(receipt.to_dict())


@ApiError.catch
async def get_uncles(request):
    """
    Get uncles list
    """
    params = validate_params(request)
    storage = request.app['storage']
    tip_hash = request.query.get('blockchain_tip') or None

    uncles, last_affected_block = await storage.get_uncles(params['limit'], params['offset'], params['order'])

    uncles, tip_meta = await maybe_apply_tip(storage, tip_hash, uncles, last_affected_block, empty=[])
    uncles = [u.to_dict() for u in uncles]

    return api_success(uncles, meta=tip_meta)


async def get_uncle(request):
    """
    Get uncle by hash or number
    """
    storage = request.app['storage']
    tag = get_tag(request)
    uncle = await storage.get_uncle(tag)
    if uncle is None:
        return api_error_response_404()
    return api_success(uncle.to_dict())


async def get_transaction(request):
    storage = request.app['storage']
    txhash = request.match_info.get('txhash')

    transaction = await storage.get_transaction(txhash)
    if transaction is None:
        return api_error_response_404()
    return api_success(transaction.to_dict())
