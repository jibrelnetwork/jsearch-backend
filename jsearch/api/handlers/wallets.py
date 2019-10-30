import logging

from aiohttp import web
from typing import Optional, List

from jsearch.api.blockchain_tip import maybe_apply_tip
from jsearch.api.error_code import ErrorCode
from jsearch.api.handlers.common import (
    get_last_block_number_and_timestamp,
    get_tip_block_number_and_timestamp,
    get_block_number_or_tag_from_timestamp
)
from jsearch.api.helpers import ApiError, maybe_orphan_request
from jsearch.api.helpers import (
    api_success,
    api_error_response,
)
from jsearch.api.ordering import Ordering
from jsearch.api.pagination import get_page
from jsearch.api.serializers.wallets import WalletEventsSchema, WalletAssetsSchema
from jsearch.api.structs.wallets import wallet_events_to_json
from jsearch.api.utils import use_kwargs
from jsearch.typing import IntOrStr, OrderScheme

logger = logging.getLogger(__name__)

MAX_COUNT = 1000
MAX_LIMIT = 1000
MAX_OFFSET = 10000
PENDING_EVENTS_DEFAULT_LIMIT = 100


def get_key_set_fields(scheme: OrderScheme):
    return ['event_index']


@ApiError.catch
@use_kwargs(WalletEventsSchema())
async def get_wallet_events(
        request: web.Request,
        address: str,
        order: Ordering,
        limit: int,
        include_pending_txs: bool = False,
        tip_hash: Optional[str] = None,
        block_number: Optional[IntOrStr] = None,
        timestamp: Optional[IntOrStr] = None,
        tx_index: Optional[int] = None,
        event_index: Optional[int] = None,
) -> web.Response:
    storage = request.app['storage']
    last_known_chain_insert_id = await storage.get_latest_chain_insert_id()

    if timestamp:
        block_number = await get_block_number_or_tag_from_timestamp(storage, timestamp, order.direction)
        timestamp = None

    block_number, timestamp = await get_last_block_number_and_timestamp(block_number, timestamp, storage)
    block_number, timestamp = await get_tip_block_number_and_timestamp(block_number, timestamp, tip_hash, storage)

    # Notes: we need to query limit + 1 items to get link on next page
    events, progress, last_affected_block = await storage.get_wallet_events(
        address=address,
        block_number=block_number,
        tx_index=tx_index,
        event_index=event_index,
        ordering=order,
        limit=limit + 1
    )

    data, tip = await maybe_apply_tip(storage, tip_hash, events, last_affected_block, empty=[])

    url = request.app.router['wallet_events'].url_for()
    page = get_page(
        url=url,
        items=data,
        limit=limit,
        ordering=order,
        key_set_fields=get_key_set_fields(order.scheme),
        url_params={
            'blockchain_address': address
        },
        mapping={'blockNumber': 'block_number'}
    )

    pending_events = []
    if include_pending_txs:
        pending_events = await storage.get_account_pending_events(
            account=address,
            limit=PENDING_EVENTS_DEFAULT_LIMIT
        )

    orphaned_request = await maybe_orphan_request(
        request,
        last_known_chain_insert_id,
        last_affected_block,
        tip and tip.last_number,
    )

    if orphaned_request is not None:
        return orphaned_request

    return api_success(
        data={
            "events": wallet_events_to_json(page.items),
            "pendingEvents": pending_events
        },
        page=page,
        progress=progress,
        meta=tip and tip.to_dict()
    )


async def get_blockchain_tip(request):
    storage = request.app['storage']

    block = await storage.get_latest_block_info()
    if block is None:
        err = {
            'error_code': ErrorCode.BLOCK_NOT_FOUND,
            'error_message': f'Blockchain tip not found'
        }
        return api_error_response(status=404, errors=[err])

    return api_success({
        'blockHash': block.hash,
        'blockNumber': block.number
    })


@ApiError.catch
@use_kwargs(WalletAssetsSchema())
async def get_assets_summary(
        request: web.Request,
        addresses: List[str],
        assets: Optional[List[str]] = None,
        tip_hash: Optional[str] = None,
) -> web.Response:
    storage = request.app['storage']
    last_known_chain_insert_id = await storage.get_latest_chain_insert_id()
    if addresses:
        summary, last_affected_block = await storage.get_wallet_assets_summary(
            addresses=addresses,
            assets=assets
        )
    else:
        summary = []
        last_affected_block = None

    data, tip = await maybe_apply_tip(storage, tip_hash, summary, last_affected_block, empty=[])

    orphaned_request = await maybe_orphan_request(
        request,
        last_known_chain_insert_id,
        last_affected_block,
        tip and tip.last_number,
    )

    if orphaned_request is not None:
        return orphaned_request

    return api_success([item.to_dict() for item in data], meta=tip and tip.to_dict())
