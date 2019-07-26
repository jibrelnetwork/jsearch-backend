import logging

from aiohttp.web_request import Request
from typing import Optional, Union

from jsearch import settings
from jsearch.api.blockchain_tip import maybe_apply_tip
from jsearch.api.error_code import ErrorCode
from jsearch.api.handlers.common import get_block_number_and_timestamp
from jsearch.api.helpers import (
    get_tag,
    validate_params,
    api_success,
    api_error_response_400,
    api_error_response_404,
    get_from_joined_string,
    get_positive_number,
    ApiError,
)
from jsearch.api.ordering import Ordering
from jsearch.api.pagination import get_page
from jsearch.api.serializers.accounts import AccountsTxsSchema
from jsearch.api.utils import use_kwargs

logger = logging.getLogger(__name__)


@ApiError.catch
async def get_accounts_balances(request):
    """
    Get ballances for list of accounts
    """
    storage = request.app['storage']
    addresses = get_from_joined_string(request.query.get('addresses'))
    tip_hash = request.query.get('blockchain_tip') or None

    if len(addresses) > settings.API_QUERY_ARRAY_MAX_LENGTH:
        return api_error_response_400(errors=[
            {
                'field': 'addresses',
                'error_code': ErrorCode.TOO_MANY_ITEMS,
                'error_message': 'Too many addresses requested'
            }
        ])

    balances, last_affected_block = await storage.get_accounts_balances(addresses)
    balances, tip_or_none = await maybe_apply_tip(storage, tip_hash, balances, last_affected_block, empty=[])
    balances = [b.to_dict() for b in balances]

    return api_success(balances)


@ApiError.catch
async def get_account(request):
    """
    Get account by address
    """
    storage = request.app['storage']
    address = request.match_info.get('address').lower()
    tag = get_tag(request)
    tip_hash = request.query.get('blockchain_tip') or None

    account, last_affected_block = await storage.get_account(address, tag)

    if account is None:
        return api_error_response_404()

    account, tip_or_none = await maybe_apply_tip(storage, tip_hash, account, last_affected_block, empty=None)
    account = {} if account is None else account.to_dict()

    return api_success(account)


@ApiError.catch
@use_kwargs(AccountsTxsSchema())
async def get_account_transactions(
        request: Request,
        address: str,
        limit: int,
        order: Ordering,
        block_number: Optional[Union[int, str]] = None,
        timestamp: Optional[int] = None,
        transaction_index: Optional[int] = None,
):
    """
    Get account transactions
    """
    storage = request.app['storage']
    block_number, timestamp = await get_block_number_and_timestamp(block_number, timestamp, request)
    tip_hash = request.query.get('blockchain_tip') or None

    txs, last_affected_block = await storage.get_account_transactions(
        address=address,
        limit=limit + 1,
        ordering=order,
        block_number=block_number,
        timestamp=timestamp,
        tx_index=transaction_index
    )

    txs, tip_or_none = await maybe_apply_tip(storage, tip_hash, txs, last_affected_block, empty=[])

    url = request.app.router['accounts_txs'].url_for(address=address)
    page = get_page(url=url, items=txs, limit=limit, ordering=order)

    return api_success(data=[x.to_dict() for x in page.items], page=page)


@ApiError.catch
async def get_account_internal_transactions(request):
    """
    Get account internal transactions
    """
    storage = request.app['storage']
    address = request.match_info.get('address').lower()
    params = validate_params(request)
    tip_hash = request.query.get('blockchain_tip') or None

    internal_txs, last_affected_block = await storage.get_account_internal_transactions(
        address,
        limit=params['limit'],
        offset=params['offset'],
        order=params['order'],
    )

    internal_txs, tip_or_none = await maybe_apply_tip(storage, tip_hash, internal_txs, last_affected_block, empty=[])
    internal_txs = [it.to_dict() for it in internal_txs]

    return api_success(internal_txs)


async def get_account_pending_transactions(request):
    """
    Get account pending transactions
    """
    storage = request.app['storage']
    address = request.match_info.get('address').lower()
    params = validate_params(request)

    pending_txs = await storage.get_account_pending_transactions(
        address,
        order=params['order'],
        limit=params['limit'],
        offset=params['offset'],
    )

    response_data = [pt.to_dict() for pt in pending_txs]
    response = api_success(response_data)

    return response


@ApiError.catch
async def get_account_logs(request):
    """
    Get contract logs
    """
    storage = request.app['storage']
    address = request.match_info.get('address').lower()
    params = validate_params(request)
    tip_hash = request.query.get('blockchain_tip') or None

    block_from = get_positive_number(request=request, attr='block_range_start')
    block_until = get_positive_number(request=request, attr='block_range_end')

    logs, last_affected_block = await storage.get_account_logs(
        address=address,
        limit=params['limit'],
        offset=params['offset'],
        order=params['order'],
        block_from=block_from,
        block_until=block_until,
    )

    logs, tip_or_none = await maybe_apply_tip(storage, tip_hash, logs, last_affected_block, empty=[])
    logs = [l.to_dict() for l in logs]

    return api_success(logs)


@ApiError.catch
async def get_account_mined_blocks(request):
    """
    Get account mined blocks
    """
    storage = request.app['storage']
    address = request.match_info.get('address').lower()
    params = validate_params(request)
    tip_hash = request.query.get('blockchain_tip') or None

    blocks, last_affected_block = await storage.get_account_mined_blocks(
        address,
        params['limit'],
        params['offset'],
        params['order'],
    )

    blocks, tip_or_none = await maybe_apply_tip(storage, tip_hash, blocks, last_affected_block, empty=[])
    blocks = [b.to_dict() for b in blocks]

    return api_success(blocks)


@ApiError.catch
async def get_account_mined_uncles(request):
    """
    Get account mined uncles
    """
    storage = request.app['storage']
    address = request.match_info.get('address').lower()
    params = validate_params(request)
    tip_hash = request.query.get('blockchain_tip') or None

    uncles, last_affected_block = await storage.get_account_mined_uncles(
        address,
        params['limit'],
        params['offset'],
        params['order']
    )

    uncles, tip_or_none = await maybe_apply_tip(storage, tip_hash, uncles, last_affected_block, empty=[])
    uncles = [u.to_dict() for u in uncles]

    return api_success(uncles)


@ApiError.catch
async def get_account_token_transfers(request):
    storage = request.app['storage']
    params = validate_params(request)
    account_address = request.match_info['address'].lower()
    tip_hash = request.query.get('blockchain_tip') or None

    transfers, last_affected_block = await storage.get_account_tokens_transfers(
        address=account_address,
        limit=params['limit'],
        offset=params['offset'],
        order=params['order']
    )

    transfers, tip_or_none = await maybe_apply_tip(storage, tip_hash, transfers, last_affected_block, empty=[])
    transfers = [t.to_dict() for t in transfers]

    return api_success(transfers)


@ApiError.catch
async def get_account_token_balance(request):
    storage = request.app['storage']
    token_address = request.match_info['token_address'].lower()
    account_address = request.match_info['address'].lower()
    tip_hash = request.query.get('blockchain_tip') or None

    holder, last_affected_block = await storage.get_account_token_balance(
        account_address=account_address,
        token_address=token_address,
    )

    if holder is None:
        return api_error_response_404()

    holder, tip_or_none = await maybe_apply_tip(storage, tip_hash, holder, last_affected_block, empty=None)
    holder = {} if holder is None else holder.to_dict()

    return api_success(holder)


@ApiError.catch
async def get_account_token_balances_multi(request):
    storage = request.app['storage']
    account_address = request.match_info['address'].lower()
    tokens_addresses = [a for a in request.query.get('tokens_addresses', '').lower().split(',') if a]
    tip_hash = request.query.get('blockchain_tip') or None

    balances, last_affected_block = await storage.get_account_tokens_balances(account_address, tokens_addresses)
    return api_success([b.to_dict() for b in balances])