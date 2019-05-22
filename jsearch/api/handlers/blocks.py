import logging

from jsearch.api.helpers import (
    get_tag,
    validate_params,
    api_success,
    api_error_404,
)

logger = logging.getLogger(__name__)


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
