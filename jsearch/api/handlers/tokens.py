import logging

from aiohttp.web_request import Request
from typing import Optional, Union

from jsearch.api.blockchain_tip import maybe_apply_tip
from jsearch.api.handlers.common import get_last_block_number_and_timestamp, get_block_number_or_tag_from_timestamp
from jsearch.api.helpers import validate_params, api_success, ApiError
from jsearch.api.ordering import Ordering
from jsearch.api.pagination import get_page
from jsearch.api.serializers.tokens import TokenTransfersSchema
from jsearch.api.utils import use_kwargs

logger = logging.getLogger(__name__)


@ApiError.catch
@use_kwargs(TokenTransfersSchema())
async def get_token_transfers(
        request: Request,
        address: str,
        limit: int,
        order: Ordering,
        tip_hash: Optional[str] = None,
        block_number: Optional[Union[int, str]] = None,
        timestamp: Optional[int] = None,
        transaction_index: Optional[int] = None,
        log_index: Optional[int] = None,
):
    storage = request.app['storage']
    if timestamp is not None:
        block_number = await get_block_number_or_tag_from_timestamp(storage, timestamp, order.direction)
    timestamp = None

    block_number, timestamp = await get_last_block_number_and_timestamp(block_number, timestamp, storage)
    transfers, last_affected_block = await storage.get_tokens_transfers(
        address=address,
        limit=limit + 1,
        ordering=order,
        block_number=block_number,
        transaction_index=transaction_index,
        log_index=log_index
    )
    transfers, tip_meta = await maybe_apply_tip(storage, tip_hash, transfers, last_affected_block, empty=[])

    url = request.app.router['token_transfers'].url_for(address=address)
    page = get_page(url=url, items=transfers, limit=limit, ordering=order)

    return api_success(data=[x.to_dict() for x in page.items], page=page, meta=tip_meta)


async def get_token_holders(request):
    storage = request.app['storage']
    params = validate_params(request)
    token_address = request.match_info['address'].lower()
    tip_hash = request.query.get('blockchain_tip') or None

    holders, last_affected_block = await storage.get_tokens_holders(
        address=token_address,
        limit=params['limit'],
        offset=params['offset'],
        order=params['order']
    )

    holders, tip_meta = await maybe_apply_tip(storage, tip_hash, holders, last_affected_block, empty=[])
    holders = [h.to_dict() for h in holders]

    return api_success(holders, meta=tip_meta)
