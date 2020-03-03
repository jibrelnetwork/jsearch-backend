import logging
from typing import Optional, List

from aiohttp import web

from jsearch.api.blockchain_tip import maybe_apply_tip
from jsearch.api.error_code import ErrorCode
from jsearch.api.handlers.common import get_last_block_number_and_timestamp
from jsearch.api.helpers import ApiError, api_success, maybe_orphan_request, api_error_response
from jsearch.api.ordering import Ordering
from jsearch.api.pagination import get_pagination_description
from jsearch.api.serializers.dex import DexHistorySchema, DexOrdersSchema, DexBlockedAmountsSchema
from jsearch.api.utils import use_kwargs

logger = logging.getLogger(__name__)

PENDING_EVENTS_DEFAULT_LIMIT = 100


@ApiError.catch
@use_kwargs(DexHistorySchema)
async def get_dex_history(
        request: web.Request,
        order: Ordering,
        limit: int,
        token_address: str,
        tip_hash: Optional[str] = None,
        event_type: Optional[List[str]] = None,
        block_number: Optional[int] = None,
        timestamp: Optional[int] = None,
        event_index: Optional[int] = None
) -> web.Response:
    storage = request.app['storage']

    any_orders = await storage.get_asset_placed_order(token_address)
    if any_orders is None:
        err = {
            'code': ErrorCode.RESOURCE_NOT_FOUND,
            'message': f'Asset {token_address} was not found'
        }
        return api_error_response(status=404, errors=[err])

    last_known_chain_event_id = await storage.get_latest_chain_event_id()

    block_number, timestamp = await get_last_block_number_and_timestamp(block_number, timestamp, storage)
    dex_history, last_affected_block = await storage.get_dex_history(
        ordering=order,
        limit=limit + 1,  # pagination
        token_address=token_address,
        event_type=event_type,
        block_number=block_number,
        timestamp=timestamp,
        event_index=event_index
    )
    url = request.app.router['dex_history'].url_for(token_address=token_address)
    page = get_pagination_description(url=url, items=dex_history, limit=limit, ordering=order)

    data, tip = await maybe_apply_tip(storage, tip_hash, dex_history, last_affected_block, empty=[])
    orphaned_request = await maybe_orphan_request(
        request,
        last_chain_event_id=last_known_chain_event_id,
        last_data_block=last_affected_block,
        last_tip_block=tip and tip.last_number,
    )

    if orphaned_request is not None:
        return orphaned_request

    return api_success(
        data=[item.as_dict() for item in dex_history[:limit]],
        page=page,
        meta=tip and tip.to_dict()
    )


@ApiError.catch
@use_kwargs(DexOrdersSchema)
async def get_dex_orders(
        request: web.Request,
        token_address: str,
        order_status: Optional[List[str]] = None,
        order_creator: Optional[str] = None,
        tip_hash: Optional[str] = None,
) -> web.Response:
    storage = request.app['storage']
    last_known_chain_event_id = await storage.get_latest_chain_event_id()

    dex_history, last_affected_block = await storage.get_dex_orders(token_address, order_creator, order_status)

    data, tip = await maybe_apply_tip(storage, tip_hash, dex_history, last_affected_block, empty=[])
    orphaned_request = await maybe_orphan_request(
        request,
        last_known_chain_event_id,
        last_affected_block,
        tip and tip.last_number,
    )

    if orphaned_request is not None:
        return orphaned_request
    return api_success(
        data=[item.as_dict() for item in dex_history],
        meta=tip and tip.to_dict()
    )


@ApiError.catch
@use_kwargs(DexBlockedAmountsSchema)
async def get_dex_blocked_amounts(
        request: web.Request,
        user_address: str,
        token_addresses: List[str] = None,
        tip_hash: Optional[str] = None,
) -> web.Response:
    storage = request.app['storage']
    last_known_chain_event_id = await storage.get_latest_chain_event_id()

    dex_history, last_affected_block = await storage.get_dex_blocked(user_address, token_addresses)

    data, tip = await maybe_apply_tip(storage, tip_hash, dex_history, last_affected_block, empty=[])
    orphaned_request = await maybe_orphan_request(
        request,
        last_known_chain_event_id,
        last_affected_block,
        tip and tip.last_number,
    )

    if orphaned_request is not None:
        return orphaned_request
    return api_success(
        data=[item._asdict() for item in dex_history],
        meta=tip and tip.to_dict()
    )
