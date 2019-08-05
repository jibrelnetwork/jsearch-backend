import asyncio
import logging

from typing import Optional

from jsearch.api.blockchain_tip import maybe_apply_tip
from jsearch.api.error_code import ErrorCode
from jsearch.api.handlers.common import (
    get_last_block_number_and_timestamp,
    get_tip_block_number_and_timestamp,
    get_block_number_or_tag_from_timestamp
)
from jsearch.api.helpers import ApiError
from jsearch.api.helpers import (
    validate_params,
    api_success,
    api_error_response,
    get_from_joined_string,
)
from jsearch.api.ordering import Ordering, ORDER_SCHEME_BY_NUMBER
from jsearch.api.pagination import get_page
from jsearch.api.serializers.wallets import WalletEventsSchema
from jsearch.api.structs.wallets import wallet_events_to_json
from jsearch.api.utils import use_kwargs
from jsearch.typing import IntOrStr, OrderScheme

logger = logging.getLogger(__name__)

MAX_COUNT = 1000
MAX_LIMIT = 1000
MAX_OFFSET = 10000
PENDING_EVENTS_DEFAULT_LIMIT = 100


def get_key_set_fields(scheme: OrderScheme):
    if scheme == ORDER_SCHEME_BY_NUMBER:
        key_set_fields = ['blockNumber', 'event_index']
    else:
        key_set_fields = ['timestamp', 'event_index']
    return key_set_fields


@ApiError.catch
@use_kwargs(WalletEventsSchema())
async def get_wallet_events(
        request,
        address: str,
        order: Ordering,
        limit: int,
        include_pending_txs: bool = False,
        tip_hash: Optional[str] = None,
        block_number: Optional[IntOrStr] = None,
        timestamp: Optional[IntOrStr] = None,
        tx_index: Optional[int] = None,
        event_index: Optional[int] = None,
):
    storage = request.app['storage']
    if timestamp:
        block_number = await get_block_number_or_tag_from_timestamp(storage, timestamp, order.direction)
        timestamp = None

    block_number, timestamp = await get_last_block_number_and_timestamp(block_number, timestamp, storage)
    block_number, timestamp = await get_tip_block_number_and_timestamp(block_number, timestamp, tip_hash, storage)

    # Notes: we need to query limit + 1 items to get link on next page
    events, last_affected_block = await storage.get_wallet_events(
        address=address,
        block_number=block_number,
        tx_index=tx_index,
        event_index=event_index,
        ordering=order,
        limit=limit + 1
    )

    data, tip_meta = await maybe_apply_tip(storage, tip_hash, events, last_affected_block, empty=[])

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

    return api_success(
        data={
            "events": wallet_events_to_json(page.items),
            "pendingEvents": pending_events
        },
        page=page,
        meta=tip_meta
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


async def get_assets_summary(request):
    addresses = get_from_joined_string(request.query.get('addresses'))
    assets = get_from_joined_string(request.query.get('assets'))
    storage = request.app['storage']
    tip_hash = request.query.get('blockchain_tip')
    if addresses:
        summary = await storage.get_wallet_assets_summary(
            addresses,
            assets=assets
        )
    else:
        summary = []
    last_affected_block = 11000
    data, tip_meta = await maybe_apply_tip(storage, tip_hash, summary, last_affected_block, empty=summary)
    return api_success([item.to_dict() for item in data], meta=tip_meta)


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
