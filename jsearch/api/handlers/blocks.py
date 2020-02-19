from aiohttp.web_request import Request
from aiohttp.web_response import Response
from typing import Optional, Union

from jsearch.api.blockchain_tip import maybe_apply_tip
from jsearch.api.handlers.common import get_last_block_number_and_timestamp
from jsearch.api.helpers import (
    get_tag,
    api_success,
    api_error_response_404,
    ApiError,
    maybe_orphan_request,
)
from jsearch.api.ordering import Ordering
from jsearch.api.pagination import get_pagination_description
from jsearch.api.serializers.blocks import BlockListSchema
from jsearch.api.utils import use_kwargs


@ApiError.catch
@use_kwargs(BlockListSchema())
async def get_blocks(
        request: Request,
        limit: int,
        order: Ordering,
        tip_hash: Optional[str] = None,
        block_number: Optional[Union[int, str]] = None,
        timestamp: Optional[Union[int, str]] = None
) -> Response:
    """
    Get blocks list
    """
    storage = request.app['storage']
    last_known_chain_event_id = await storage.get_latest_chain_event_id()

    block_number, timestamp = await get_last_block_number_and_timestamp(block_number, timestamp, storage)
    # Notes: we need to query limit + 1 items to get link on next page
    blocks, progress, last_affected_block = await storage.get_blocks(
        limit=limit + 1,
        number=block_number,
        timestamp=timestamp,
        order=order,
    )

    blocks, tip = await maybe_apply_tip(storage, tip_hash, blocks, last_affected_block, empty=[])

    url = request.app.router['blocks'].url_for()
    page = get_pagination_description(
        url=url,
        items=blocks,
        limit=limit,
        ordering=order,
        mapping=BlockListSchema.mapping
    )

    orphaned_request = await maybe_orphan_request(
        request,
        last_known_chain_event_id,
        last_affected_block,
        tip and tip.last_number,
    )

    if orphaned_request is not None:
        return orphaned_request

    return api_success(data=[x.to_dict() for x in page.items], page=page, progress=progress, meta=tip and tip.to_dict())


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


async def get_block_transaction_count(request):
    storage = request.app['storage']
    tag = get_tag(request)
    block = await storage.get_block(tag)
    return api_success({"count": len(block['transactions'])})


async def get_block_uncles(request):
    storage = request.app['storage']
    tag = get_tag(request)
    uncles = await storage.get_block_uncles(tag)
    return api_success([u.to_dict() for u in uncles])


async def get_block_internal_transactions(request):
    storage = request.app['storage']
    tag = get_tag(request)
    parent_tx_hash = request.query.get('parent_tx_hash')
    txs = await storage.get_block_internal_transactions(tag, parent_tx_hash=parent_tx_hash)
    return api_success([t.to_dict() for t in txs])
