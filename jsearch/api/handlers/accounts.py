import logging

from aiohttp import web
from aiohttp.web_request import Request
from typing import Optional, Union

from jsearch import settings
from jsearch.api.blockchain_tip import maybe_apply_tip
from jsearch.api.error_code import ErrorCode
from jsearch.api.handlers.common import get_last_block_number_and_timestamp, get_block_number_or_tag_from_timestamp
from jsearch.api.helpers import (
    get_tag,
    validate_params,
    api_success,
    api_error_response_400,
    api_error_response_404,
    get_from_joined_string,
    ApiError,
)
from jsearch.api.ordering import Ordering
from jsearch.api.pagination import get_page
from jsearch.api.serializers.accounts import (
    AccountsTxsSchema,
    AccountLogsSchema,
    AccountsInternalTxsSchema,
    AccountsPendingTxsSchema,
    AccountsTransfersSchema
)
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
    balances, tip_meta = await maybe_apply_tip(storage, tip_hash, balances, last_affected_block, empty=[])
    balances = [b.to_dict() for b in balances]

    return api_success(balances, meta=tip_meta)


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

    account, tip_meta = await maybe_apply_tip(storage, tip_hash, account, last_affected_block, empty=None)
    account = {} if account is None else account.to_dict()

    return api_success(account, meta=tip_meta)


@ApiError.catch
@use_kwargs(AccountsTxsSchema())
async def get_account_transactions(
        request: Request,
        address: str,
        limit: int,
        order: Ordering,
        tip_hash: Optional[str] = None,
        block_number: Optional[Union[int, str]] = None,
        timestamp: Optional[int] = None,
        transaction_index: Optional[int] = None,
):
    """
    Get account transactions
    """
    storage = request.app['storage']
    block_number, timestamp = await get_last_block_number_and_timestamp(block_number, timestamp, storage)

    txs, last_affected_block = await storage.get_account_transactions(
        address=address,
        limit=limit + 1,
        ordering=order,
        block_number=block_number,
        timestamp=timestamp,
        tx_index=transaction_index
    )

    txs, tip_meta = await maybe_apply_tip(storage, tip_hash, txs, last_affected_block, empty=[])

    url = request.app.router['accounts_txs'].url_for(address=address)
    page = get_page(url=url, items=txs, limit=limit, ordering=order)

    return api_success(data=[x.to_dict() for x in page.items], page=page, meta=tip_meta)


@ApiError.catch
@use_kwargs(AccountsInternalTxsSchema())
async def get_account_internal_transactions(
        request: Request,
        address: str,
        limit: int,
        order: Ordering,
        tip_hash: Optional[str] = None,
        block_number: Optional[Union[int, str]] = None,
        timestamp: Optional[int] = None,
        parent_transaction_index: Optional[int] = None,
        transaction_index: Optional[int] = None,
):
    """
    Get account internal transactions
    """
    storage = request.app['storage']
    block_number, timestamp = await get_last_block_number_and_timestamp(block_number, timestamp, storage)
    txs, last_affected_block = await storage.get_account_internal_transactions(
        address=address,
        limit=limit + 1,
        ordering=order,
        block_number=block_number,
        timestamp=timestamp,
        parent_tx_index=parent_transaction_index,
        tx_index=transaction_index
    )
    txs, tip_meta = await maybe_apply_tip(storage, tip_hash, txs, last_affected_block, empty=[])

    url = request.app.router['accounts_internal_txs'].url_for(address=address)
    page = get_page(url=url, items=txs, limit=limit, ordering=order, mapping=AccountsInternalTxsSchema.mapping)

    return api_success(data=[x.to_dict() for x in page.items], page=page, meta=tip_meta)


@ApiError.catch
@use_kwargs(AccountsPendingTxsSchema())
async def get_account_pending_transactions(
        request,
        limit: int,
        order: Ordering,
        address: str,
        timestamp: Optional[int] = None,
        tx_id: Optional[int] = None
):
    """
    Get account pending transactions
    """
    storage = request.app['storage']
    if timestamp is None:
        timestamp = await storage.get_account_pending_tx_timestamp(address, order)

    # Notes: we need to query limit + 1 items to get link on next page
    txs = [] if timestamp is None else await storage.get_account_pending_transactions(
        account=address,
        limit=limit + 1,
        ordering=order,
        timestamp=timestamp,
        id=tx_id
    )

    url = request.app.router['accounts_pending_txs'].url_for(address=address)
    page = get_page(url=url, items=txs, limit=limit, ordering=order, mapping=AccountsPendingTxsSchema.mapping)

    return api_success(data=[x.to_dict() for x in page.items], page=page)


@ApiError.catch
@use_kwargs(AccountLogsSchema())
async def get_account_logs(
        request: web.Request,
        address: str,
        limit: int,
        order: Ordering,
        block_number: Optional[Union[int, str]] = None,
        timestamp: Optional[Union[int, str]] = None,
        transaction_index: Optional[int] = None,
        log_index: Optional[int] = None,
        tip_hash: Optional[str] = None,
) -> web.Response:
    """
    Get contract logs
    """
    storage = request.app['storage']

    block_number, timestamp = await get_last_block_number_and_timestamp(block_number, timestamp, storage)
    logs, last_affected_block = await storage.get_account_logs(
        address=address,
        limit=limit + 1,
        ordering=order,
        block_number=block_number,
        timestamp=timestamp,
        transaction_index=transaction_index,
        log_index=log_index,
    )
    logs, tip_meta = await maybe_apply_tip(storage, tip_hash, logs, last_affected_block, empty=[])

    url = request.app.router['accounts_logs'].url_for(address=address)
    page = get_page(url=url, items=logs, limit=limit, ordering=order)

    return api_success(data=[x.to_dict() for x in page.items], page=page, meta=tip_meta)


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

    blocks, tip_meta = await maybe_apply_tip(storage, tip_hash, blocks, last_affected_block, empty=[])
    blocks = [b.to_dict() for b in blocks]

    return api_success(blocks, meta=tip_meta)


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

    uncles, tip_meta = await maybe_apply_tip(storage, tip_hash, uncles, last_affected_block, empty=[])
    uncles = [u.to_dict() for u in uncles]

    return api_success(uncles, meta=tip_meta)


@ApiError.catch
@use_kwargs(AccountsTransfersSchema())
async def get_account_token_transfers(
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
    transfers, last_affected_block = await storage.get_account_tokens_transfers(
        address=address,
        limit=limit + 1,
        ordering=order,
        block_number=block_number,
        transaction_index=transaction_index,
        log_index=log_index
    )
    transfers, tip_meta = await maybe_apply_tip(storage, tip_hash, transfers, last_affected_block, empty=[])

    url = request.app.router['account_transfers'].url_for(address=address)
    page = get_page(url=url, items=transfers, limit=limit, ordering=order)

    return api_success(data=[x.to_dict() for x in page.items], page=page, meta=tip_meta)


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

    holder, tip_meta = await maybe_apply_tip(storage, tip_hash, holder, last_affected_block, empty=None)
    holder = {} if holder is None else holder.to_dict()

    return api_success(holder, meta=tip_meta)


@ApiError.catch
async def get_account_token_balances_multi(request):
    storage = request.app['storage']
    account_address = request.match_info['address'].lower()
    tokens_addresses = get_from_joined_string(request.query.get('tokens_addresses'))
    tip_hash = request.query.get('blockchain_tip') or None

    if len(tokens_addresses) > settings.API_QUERY_ARRAY_MAX_LENGTH:
        return api_error_response_400(errors=[
            {
                'field': 'tokens_addresses',
                'error_code': ErrorCode.TOO_MANY_ITEMS,
                'error_message': 'Too many addresses requested'
            }
        ])

    balances, last_affected_block = await storage.get_account_tokens_balances(account_address, tokens_addresses)
    balances, tip_meta = await maybe_apply_tip(storage, tip_hash, balances, last_affected_block, empty=[])
    return api_success([b.to_dict() for b in balances], meta=tip_meta)


@ApiError.catch
async def get_account_transaction_count(request):
    storage = request.app['storage']
    account_address = request.match_info['address'].lower()
    include_pending_txs = not request.query.get('include_pending_txs') == 'false'

    tx_count = await storage.get_account_transaction_count(account_address, include_pending_txs)
    return api_success(tx_count)
