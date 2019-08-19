import logging

from aiohttp import web

from jsearch.api.helpers import (
    api_success,
    api_error_response_404,
    ApiError)
from jsearch.api.serializers.explorer import InternalTransactionsSchema
from jsearch.api.utils import use_kwargs

logger = logging.getLogger(__name__)


@ApiError.catch
@use_kwargs(InternalTransactionsSchema())
async def get_internal_transactions(
        request: web.Request,
        txhash: str,
        order: str,
) -> web.Response:
    """
    Get internal transactions by transaction hash
    """
    storage = request.app['storage']
    internal_txs = await storage.get_internal_transactions(txhash, order)

    response_data = [it.to_dict() for it in internal_txs]
    response = api_success(response_data)

    return response


async def get_receipt(request):
    storage = request.app['storage']
    txhash = request.match_info.get('txhash')

    receipt = await storage.get_receipt(txhash)
    if receipt is None:
        return api_error_response_404()
    return api_success(receipt.to_dict())


async def get_transaction(request):
    storage = request.app['storage']
    txhash = request.match_info.get('txhash')

    transaction = await storage.get_transaction(txhash)
    if transaction is None:
        return api_error_response_404()
    return api_success(transaction.to_dict())
