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
)
from jsearch.api.ordering import Ordering
from jsearch.api.pagination import get_page
from jsearch.api.serializers.uncles import UncleListSchema
from jsearch.api.utils import use_kwargs


@ApiError.catch
@use_kwargs(UncleListSchema())
async def get_uncles(
        request: Request,
        limit: int,
        order: Ordering,
        tip_hash: Optional[str] = None,
        uncle_number: Optional[Union[int, str]] = None,
        timestamp: Optional[Union[int, str]] = None
) -> Response:
    """
    Get uncles list
    """
    storage = request.app['storage']
    block_number, timestamp = await get_last_block_number_and_timestamp(uncle_number, timestamp, storage)

    # Notes: we need to query limit + 1 items to get link on next page
    uncles, last_affected_block = await storage.get_uncles(
        limit=limit + 1,
        number=block_number,
        timestamp=timestamp,
        order=order,
    )

    uncles, tip_meta = await maybe_apply_tip(storage, tip_hash, uncles, last_affected_block, empty=[])

    url = request.app.router['uncles'].url_for()
    page = get_page(url=url, items=uncles, limit=limit, ordering=order, mapping=UncleListSchema.mapping)

    return api_success(data=[x.to_dict() for x in page.items], page=page, meta=tip_meta)


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
