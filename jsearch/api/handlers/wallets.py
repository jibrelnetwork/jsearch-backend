import asyncio
import logging

from aiohttp import web
from functools import partial
from typing import Tuple, Optional

from jsearch.api.error_code import ErrorCode
from jsearch.api.helpers import ApiError, get_positive_number
from jsearch.api.helpers import (
    validate_params,
    api_success,
    api_error_response,
    get_from_joined_string,
)
from jsearch.api.ordering import ORDER_ASC
from jsearch.api.structs import BlockInfo

logger = logging.getLogger(__name__)

MAX_COUNT = 1000
MAX_LIMIT = 1000
MAX_OFFSET = 10000
PENDING_EVENTS_DEFAULT_LIMIT = 100


def get_address(request) -> str:
    address = request.query.get('blockchain_address', '').lower()
    if not address:
        raise ApiError(
            {
                'param': 'blockchain_address',
                'error_code': ErrorCode.VALIDATION_ERROR,
                'error_message': f'Query param `blockchain_address` is required'
            },
            status=400
        )
    return address


def get_tip_hash(request) -> Optional[str]:
    tip = request.query.get('blockchain_tip')
    if tip is None:
        raise ApiError(
            {
                'param': 'blockchain_tip',
                'error_code': ErrorCode.VALIDATION_ERROR,
                'error_message': f'Query param `blockchain_tip` is required'
            },
            status=400
        )

    return tip


async def get_tip_block(storage, block_hash: str) -> BlockInfo:
    block_info = await storage.get_block_number(block_hash)
    if block_info is None:
        raise ApiError(
            {
                'field': 'tip',
                'error_code': ErrorCode.BLOCK_NOT_FOUND,
                'error_message': f'Block with hash {block_hash} not found'
            },
            status=404
        )
    return block_info


def get_block_range(request: web.Request,
                    tip_block: BlockInfo,
                    latest_block: BlockInfo,
                    is_asc_order: bool) -> Tuple[int, int]:
    get_block_number = partial(get_positive_number, tags={"latest": latest_block.number, "tip": tip_block.number})

    until_to = get_block_number(request, 'block_range_end')
    start_from = get_block_number(request, 'block_range_start', is_required=True)

    count = get_positive_number(request, 'block_range_count')

    # set default value only if count is None
    if count is None and until_to is None:
        until_to = tip_block.number

    if count is not None:

        if count < 0:
            count = 0

        if count is not None and count > 0:
            count -= 1

        if until_to is None:
            offset = count if is_asc_order else count * -1
            until_to = start_from + offset

        is_reversed = start_from > until_to if is_asc_order else start_from < until_to
        if is_reversed:
            start_from, until_to = until_to, start_from

        if is_asc_order:
            until_to = min(start_from + count, until_to)
        else:
            until_to = max(start_from - count, until_to)

    return start_from, until_to


@ApiError.catch
async def get_wallet_events(request):
    storage = request.app['storage']
    params = validate_params(request, max_limit=MAX_LIMIT, max_offset=MAX_OFFSET, default_order='asc')

    address = get_address(request)
    tip_hash = get_tip_hash(request)

    tip_block = await get_tip_block(storage, block_hash=tip_hash)
    latest_block = await storage.get_latest_block_info()

    start_from, until_to = get_block_range(request, tip_block, latest_block, is_asc_order=params['order'] == ORDER_ASC)

    events = await storage.get_wallet_events(address, start_from, until_to, **params)

    tip = await storage.get_blockchain_tip(tip=tip_block, last_block=latest_block)
    is_event_affected = (
            tip.is_in_fork and
            tip.last_unchanged_block is not None and
            max(until_to, start_from) > tip.last_unchanged_block
    )
    events = not is_event_affected and events or []

    pending_events = []
    include_pending_events = request.query.get('include_pending_events', False)
    if include_pending_events:
        pending_events = await storage.get_account_pending_events(
            address,
            order=ORDER_ASC,
            limit=PENDING_EVENTS_DEFAULT_LIMIT
        )

    return api_success({
        "blockchainTip": tip.to_dict(),
        "events": events,
        "pending_events": pending_events
    })


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
    params = validate_params(request)
    addresses = get_from_joined_string(request.query.get('addresses'))
    assets = get_from_joined_string(request.query.get('assets'))
    storage = request.app['storage']
    if addresses:
        summary = await storage.get_wallet_assets_summary(
            addresses,
            limit=params['limit'],
            offset=params['offset'],
            assets=assets
        )
    else:
        summary = []
    return api_success([item.to_dict() for item in summary])


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
