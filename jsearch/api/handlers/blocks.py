from aiohttp.web_request import Request
from aiohttp.web_response import Response
from typing import Optional, Union

from jsearch.api.blockchain_tip import get_tip_or_raise_api_error, is_tip_stale
from jsearch.api.helpers import (
    get_tag,
    api_success,
    api_error_response_404,
    Tag,
    ApiError)
from jsearch.api.pagination import get_page
from jsearch.api.serializers.blocks import BlocksSchema
from jsearch.api.structs import Ordering
from jsearch.api.utils import use_kwargs


@ApiError.catch
@use_kwargs(BlocksSchema())
async def get_blocks(
        request: Request,
        limit: int,
        order: Ordering,
        number: Optional[Union[int, str]] = None,
        timestamp: Optional[Union[int, str]] = None
) -> Response:
    """
    Get blocks list
    """
    storage = request.app['storage']

    if {number, timestamp} & {Tag.LATEST}:
        last_block = await storage.get_latest_block_info()
        number = number and last_block.number
        timestamp = timestamp and last_block.timestamp

    # Notes: we need to query limit + 1 items to get link on next page
    blocks, last_affected_block = await storage.get_blocks(
        limit=limit + 1,
        number=number,
        timestamp=timestamp,
        order=order,
    )

    data = [block.to_dict() for block in blocks]

    tip_hash = request.query.get('blockchain_tip') or None
    tip = tip_hash and await get_tip_or_raise_api_error(storage, tip_hash)
    tip_is_stale = is_tip_stale(tip, last_affected_block)

    data = [] if tip_is_stale else data

    url = request.app.router['blocks'].url_for()
    page = get_page(url=url, items=data, limit=limit, ordering=order, mapping=BlocksSchema.mapping)

    return api_success(data=page.items, page=page)


async def get_block(request):
    """
    Get block by hash or number
    """
    storage = request.app['storage']
    tag = get_tag(request)
    block = await storage.get_block(tag)
    if block is None:
        return api_error_response_404()
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
