import logging

from aiohttp import web
from aiohttp.web_request import Request
from aiohttp.web_response import Response
from typing import Optional, Union

from jsearch import settings
from jsearch.api.blockchain_tip import maybe_apply_tip
from jsearch.api.error_code import ErrorCode
from jsearch.api.handlers.common import get_last_block_number_and_timestamp, get_block_number_or_tag_from_timestamp
from jsearch.api.helpers import (
    get_tag,
    api_success,
    api_error_response_400,
    api_error_response_404,
    get_from_joined_string,
    ApiError,
    maybe_orphan_request)
from jsearch.api.ordering import Ordering, OrderScheme
from jsearch.api.pagination import get_pagination_description
from jsearch.api.serializers.accounts import (
    AccountsTxsSchema,
    AccountLogsSchema,
    AccountsInternalTxsSchema,
    AccountMinedBlocksSchema,
    AccountsPendingTxsSchema,
    EthTransfersListSchema,
    AccountsTransfersSchema
)
from jsearch.api.serializers.uncles import (
    AccountUncleSchema,
)
from jsearch.api.utils import use_kwargs

logger = logging.getLogger(__name__)


@ApiError.catch
async def get_accounts_balances(request):
    """
    Get ballances for list of accounts
    """
    storage = request.app['storage']
    last_known_chain_event_id = await storage.get_latest_chain_event_id()
    addresses = get_from_joined_string(request.query.get('addresses'), to_lower_case=True)
    tip_hash = request.query.get('blockchain_tip') or None

    if len(addresses) > settings.API_QUERY_ARRAY_MAX_LENGTH:
        return api_error_response_400(errors=[
            {
                'field': 'addresses',
                'code': ErrorCode.TOO_MANY_ITEMS,
                'message': 'Too many addresses requested'
            }
        ])

    balances, last_affected_block = await storage.get_accounts_balances(addresses)
    balances, tip = await maybe_apply_tip(storage, tip_hash, balances, last_affected_block, empty=[])
    balances = [b.to_dict() for b in balances]

    orphaned_request = await maybe_orphan_request(
        request,
        last_known_chain_event_id,
        last_affected_block,
        tip and tip.last_number,
    )

    if orphaned_request is not None:
        return orphaned_request

    return api_success(balances, meta=tip and tip.to_dict())


@ApiError.catch
async def get_account(request):
    """
    Get account by address
    """
    storage = request.app['storage']
    last_known_chain_event_id = await storage.get_latest_chain_event_id()

    address = request.match_info.get('address').lower()
    tag = get_tag(request)
    tip_hash = request.query.get('blockchain_tip') or None

    account, last_affected_block = await storage.get_account(address, tag)

    if account is None:
        return api_error_response_404()

    account, tip = await maybe_apply_tip(storage, tip_hash, account, last_affected_block, empty=None)
    account = {} if account is None else account.to_dict()

    orphaned_request = await maybe_orphan_request(
        request,
        last_known_chain_event_id,
        last_affected_block,
        tip and tip.last_number,
    )

    if orphaned_request is not None:
        return orphaned_request

    return api_success(account, meta=tip and tip.to_dict())


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
    last_known_chain_event_id = await storage.get_latest_chain_event_id()

    block_number, timestamp = await get_last_block_number_and_timestamp(block_number, timestamp, storage)

    txs, last_affected_block = await storage.get_account_transactions(
        address=address,
        limit=limit + 1,
        ordering=order,
        block_number=block_number,
        timestamp=timestamp,
        tx_index=transaction_index
    )

    txs, tip = await maybe_apply_tip(storage, tip_hash, txs, last_affected_block, empty=[])

    url = request.app.router['accounts_txs'].url_for(address=address)
    page = get_pagination_description(url=url, items=txs, limit=limit, ordering=order)

    orphaned_request = await maybe_orphan_request(
        request,
        last_known_chain_event_id,
        last_affected_block,
        tip and tip.last_number,
    )

    if orphaned_request is not None:
        return orphaned_request

    return api_success(data=[x.to_dict() for x in page.items], page=page, meta=tip and tip.to_dict())


@ApiError.catch
@use_kwargs(AccountsInternalTxsSchema())
async def get_account_internal_txs(
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
    last_known_chain_event_id = await storage.get_latest_chain_event_id()

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
    txs, tip = await maybe_apply_tip(storage, tip_hash, txs, last_affected_block, empty=[])

    url = request.app.router['accounts_internal_txs'].url_for(address=address)
    page = get_pagination_description(url=url, items=txs, limit=limit, ordering=order,
                                      mapping=AccountsInternalTxsSchema.mapping)

    orphaned_request = await maybe_orphan_request(
        request,
        last_known_chain_event_id,
        last_affected_block,
        tip and tip.last_number,
    )

    if orphaned_request is not None:
        return orphaned_request

    return api_success(data=[x.to_dict() for x in page.items], page=page, meta=tip and tip.to_dict())


@ApiError.catch
@use_kwargs(AccountsPendingTxsSchema())
async def get_account_pending_txs(
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
    page = get_pagination_description(url=url, items=txs, limit=limit, ordering=order,
                                      mapping=AccountsPendingTxsSchema.mapping)

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
        topics: Optional[str] = None,
) -> web.Response:
    """
    Get contract logs
    """
    storage = request.app['storage']
    topics = get_from_joined_string(topics)
    last_known_chain_event_id = await storage.get_latest_chain_event_id()

    block_number, timestamp = await get_last_block_number_and_timestamp(block_number, timestamp, storage)
    logs, last_affected_block = await storage.get_account_logs(
        address=address,
        limit=limit + 1,
        ordering=order,
        topics=topics,
        block_number=block_number,
        timestamp=timestamp,
        transaction_index=transaction_index,
        log_index=log_index,
    )
    logs, tip = await maybe_apply_tip(storage, tip_hash, logs, last_affected_block, empty=[])

    url = request.app.router['accounts_logs'].url_for(address=address)
    page = get_pagination_description(url=url, items=logs, limit=limit, ordering=order)

    orphaned_request = await maybe_orphan_request(
        request,
        last_known_chain_event_id,
        last_affected_block,
        tip and tip.last_number,
    )

    if orphaned_request is not None:
        return orphaned_request

    return api_success(data=[x.to_dict() for x in page.items], page=page, meta=tip and tip.to_dict())


@ApiError.catch
@use_kwargs(AccountMinedBlocksSchema())
async def get_account_mined_blocks(
        request: web.Request,
        address: str,
        limit: int,
        order: Ordering,
        block_number: Optional[Union[int, str]] = None,
        timestamp: Optional[Union[int, str]] = None,
        tip_hash: Optional[str] = None,
) -> web.Response:
    """
    Get account mined blocks
    """
    storage = request.app['storage']
    last_known_chain_event_id = await storage.get_latest_chain_event_id()

    block_number, timestamp = await get_last_block_number_and_timestamp(block_number, timestamp, storage)
    blocks, last_affected_block = await storage.get_account_mined_blocks(
        address,
        limit + 1,
        order,
        timestamp,
        block_number,
    )

    blocks, tip = await maybe_apply_tip(storage, tip_hash, blocks, last_affected_block, empty=[])

    url = request.app.router['accounts_mined_blocks'].url_for(address=address)
    page = get_pagination_description(
        url=url,
        items=blocks,
        limit=limit,
        ordering=order,
        mapping=AccountMinedBlocksSchema.mapping
    )

    orphaned_request = await maybe_orphan_request(
        request,
        last_known_chain_event_id,
        last_affected_block,
        tip and tip.last_number,
    )

    if orphaned_request is not None:
        return orphaned_request

    return api_success(data=[x.to_dict() for x in page.items], page=page, meta=tip and tip.to_dict())


@ApiError.catch
@use_kwargs(AccountUncleSchema())
async def get_account_mined_uncles(
        request: Request,
        address: str,
        limit: int,
        order: Ordering,
        tip_hash: Optional[str] = None,
        uncle_number: Optional[Union[int, str]] = None,
        timestamp: Optional[Union[int, str]] = None
) -> Response:
    """
    Get account mined uncles
    """
    storage = request.app['storage']
    last_known_chain_event_id = await storage.get_latest_chain_event_id()

    block_number, timestamp = await get_last_block_number_and_timestamp(uncle_number, timestamp, storage)

    # Notes: we need to query limit + 1 items to get link on next page
    uncles, last_affected_block = await storage.get_account_mined_uncles(
        limit=limit + 1,
        number=block_number,
        timestamp=timestamp,
        order=order,
        address=address,
    )

    uncles, tip = await maybe_apply_tip(storage, tip_hash, uncles, last_affected_block, empty=[])

    url = request.app.router['accounts_mined_uncles'].url_for(address=address)
    page = get_pagination_description(
        url=url,
        items=uncles,
        limit=limit,
        ordering=order,
        mapping=AccountUncleSchema.mapping
    )

    orphaned_request = await maybe_orphan_request(
        request,
        last_known_chain_event_id,
        last_affected_block,
        tip and tip.last_number,
    )

    if orphaned_request is not None:
        return orphaned_request

    return api_success(data=[x.to_dict() for x in page.items], page=page, meta=tip and tip.to_dict())


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
    last_known_chain_event_id = await storage.get_latest_chain_event_id()

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
    transfers, tip = await maybe_apply_tip(storage, tip_hash, transfers, last_affected_block, empty=[])

    url = request.app.router['account_transfers'].url_for(address=address)
    page = get_pagination_description(url=url, items=transfers, limit=limit, ordering=order)

    orphaned_request = await maybe_orphan_request(
        request,
        last_known_chain_event_id,
        last_affected_block,
        tip and tip.last_number,
    )

    if orphaned_request is not None:
        return orphaned_request

    return api_success(data=[x.to_dict() for x in page.items], page=page, meta=tip and tip.to_dict())


@ApiError.catch
async def get_account_token_balance(request):
    storage = request.app['storage']
    last_known_chain_event_id = await storage.get_latest_chain_event_id()

    token_address = request.match_info['contract_address'].lower()
    account_address = request.match_info['address'].lower()
    tip_hash = request.query.get('blockchain_tip') or None

    holder, last_affected_block = await storage.get_account_token_balance(
        account_address=account_address,
        token_address=token_address,
    )

    if holder is None:
        return api_error_response_404()

    holder, tip = await maybe_apply_tip(storage, tip_hash, holder, last_affected_block, empty=None)
    holder = {} if holder is None else holder.to_dict()

    orphaned_request = await maybe_orphan_request(
        request,
        last_known_chain_event_id,
        last_affected_block,
        tip and tip.last_number,
    )

    if orphaned_request is not None:
        return orphaned_request

    return api_success(holder, meta=tip and tip.to_dict())


@ApiError.catch
async def get_account_token_balances_multi(request):
    storage = request.app['storage']
    last_known_chain_event_id = await storage.get_latest_chain_event_id()

    account_address = request.match_info['address'].lower()
    tokens_addresses = get_from_joined_string(request.query.get('contract_addresses'), to_lower_case=True)
    tip_hash = request.query.get('blockchain_tip') or None

    if len(tokens_addresses) > settings.API_QUERY_ARRAY_MAX_LENGTH:
        return api_error_response_400(errors=[
            {
                'field': 'contract_addresses',
                'code': ErrorCode.TOO_MANY_ITEMS,
                'message': 'Too many addresses requested'
            }
        ])

    balances, last_affected_block = await storage.get_account_tokens_balances(account_address, tokens_addresses)
    balances, tip = await maybe_apply_tip(storage, tip_hash, balances, last_affected_block, empty=[])

    orphaned_request = await maybe_orphan_request(
        request,
        last_known_chain_event_id,
        last_affected_block,
        tip and tip.last_number,
    )

    if orphaned_request is not None:
        return orphaned_request

    return api_success([b.to_dict() for b in balances], meta=tip and tip.to_dict())


@ApiError.catch
async def get_account_transaction_count(request):
    storage = request.app['storage']
    account_address = request.match_info['address'].lower()
    include_pending_txs = not request.query.get('include_pending_txs') == 'false'

    tx_count = await storage.get_account_transaction_count(account_address, include_pending_txs)
    return api_success(tx_count)


def get_key_set_fields(scheme: OrderScheme):
    return ['event_index']


@ApiError.catch
@use_kwargs(EthTransfersListSchema())
async def get_account_eth_transfers(request,
                                    address,
                                    tip_hash=None,
                                    block_number=None,
                                    event_index=None,
                                    timestamp=None,
                                    order=None,
                                    limit=None):
    storage = request.app['storage']
    last_known_chain_event_id = await storage.get_latest_chain_event_id()

    if timestamp:
        block_number = await get_block_number_or_tag_from_timestamp(storage, timestamp, order.direction)
        timestamp = None

    block_number, timestamp = await get_last_block_number_and_timestamp(block_number, timestamp, storage)

    transfers, last_affected_block = await storage.get_account_eth_transfers(address,
                                                                             block_number=block_number,
                                                                             event_index=event_index,
                                                                             order=order,
                                                                             limit=limit + 1)
    transfers, tip = await maybe_apply_tip(storage, tip_hash, transfers, last_affected_block, empty=[])
    url = request.app.router['accounts_eth_transfers'].url_for(address=address)
    page = get_pagination_description(
        url=url,
        items=transfers,
        key_set_fields=get_key_set_fields(order.scheme),
        limit=limit,
        ordering=order,
        mapping=EthTransfersListSchema.mapping
    )
    data = []
    for item in page.items:
        d = item.to_dict()
        del d['block_number']
        del d['event_index']
        data.append(d)

    orphaned_request = await maybe_orphan_request(
        request,
        last_known_chain_event_id,
        last_affected_block,
        tip and tip.last_number,
    )

    if orphaned_request is not None:
        return orphaned_request

    return api_success(data=data, page=page, meta=tip and tip.to_dict())
