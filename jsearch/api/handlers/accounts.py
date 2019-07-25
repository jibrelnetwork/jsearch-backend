import logging

from aiohttp.web_request import Request
from typing import Optional, Union

from jsearch import settings
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
    ApiError)
from jsearch.api.ordering import Ordering
from jsearch.api.pagination import get_page
from jsearch.api.serializers.accounts import AccountsTxsSchema
from jsearch.api.utils import use_kwargs

logger = logging.getLogger(__name__)


async def get_accounts_balances(request):
    """
    Get ballances for list of accounts
    """
    storage = request.app['storage']
    addresses = get_from_joined_string(request.query.get('addresses'))

    if len(addresses) > settings.API_QUERY_ARRAY_MAX_LENGTH:
        return api_error_response_400(errors=[
            {
                'field': 'addresses',
                'error_code': ErrorCode.TOO_MANY_ITEMS,
                'error_message': 'Too many addresses requested'
            }
        ])

    balances, last_affected_block = await storage.get_accounts_balances(addresses)
    return api_success([b.to_dict() for b in balances])


async def get_account(request):
    """
    Get account by address
    """
    storage = request.app['storage']
    address = request.match_info.get('address').lower()
    tag = get_tag(request)

    account, last_affected_block = await storage.get_account(address, tag)
    if account is None:
        return api_error_response_404()
    return api_success(account.to_dict())


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

    txs, last_affected_block = await storage.get_account_transactions(
        address=address,
        limit=limit + 1,
        ordering=order,
        block_number=block_number,
        timestamp=timestamp,
        tx_index=transaction_index
    )
    url = request.app.router['accounts_txs'].url_for(address=address)
    page = get_page(url=url, items=txs, limit=limit, ordering=order)
    return api_success(data=[x.to_dict() for x in page.items], page=page)


async def get_account_internal_transactions(request):
    """
    Get account internal transactions
    """
    storage = request.app['storage']
    address = request.match_info.get('address').lower()
    params = validate_params(request)

    internal_txs, last_affected_block = await storage.get_account_internal_transactions(
        address,
        limit=params['limit'],
        offset=params['offset'],
        order=params['order'],
    )

    response_data = [it.to_dict() for it in internal_txs]
    response = api_success(response_data)

    return response


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


async def get_account_logs(request):
    """
    Get contract logs
    """
    storage = request.app['storage']
    address = request.match_info.get('address').lower()
    params = validate_params(request)

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
    return api_success([item.to_dict() for item in logs])


async def get_account_mined_blocks(request):
    """
    Get account mined blocks
    """
    storage = request.app['storage']
    address = request.match_info.get('address').lower()
    params = validate_params(request)

    blocks, last_affected_block = await storage.get_account_mined_blocks(
        address,
        params['limit'],
        params['offset'],
        params['order'],
    )
    return api_success([b.to_dict() for b in blocks])


async def get_account_mined_uncles(request):
    """
    Get account mined uncles
    """
    storage = request.app['storage']
    address = request.match_info.get('address').lower()
    params = validate_params(request)

    uncles, last_affected_block = await storage.get_account_mined_uncles(
        address,
        params['limit'],
        params['offset'],
        params['order']
    )
    return api_success([u.to_dict() for u in uncles])


async def get_account_token_transfers(request):
    storage = request.app['storage']
    params = validate_params(request)
    account_address = request.match_info['address'].lower()

    transfers, last_affected_block = await storage.get_account_tokens_transfers(
        address=account_address,
        limit=params['limit'],
        offset=params['offset'],
        order=params['order']
    )
    return api_success([transfer.to_dict() for transfer in transfers])


async def get_account_token_balance(request):
    storage = request.app['storage']
    token_address = request.match_info['token_address'].lower()
    account_address = request.match_info['address'].lower()

    holder, last_affected_block = await storage.get_account_token_balance(
        account_address=account_address,
        token_address=token_address,
    )
    if holder is None:
        return api_error_response_404()
    return api_success(holder.to_dict())
