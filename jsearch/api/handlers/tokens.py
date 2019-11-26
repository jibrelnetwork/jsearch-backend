import logging

from aiohttp.web_request import Request
from typing import Optional, Union

from jsearch.api.blockchain_tip import maybe_apply_tip
from jsearch.api.handlers.common import get_last_block_number_and_timestamp, get_block_number_or_tag_from_timestamp
from jsearch.api.helpers import api_success, ApiError, maybe_orphan_request
from jsearch.api.ordering import Ordering
from jsearch.api.pagination import get_page
from jsearch.api.serializers.tokens import TokenTransfersSchema, TokenHoldersListSchema
from jsearch.api.utils import use_kwargs
from jsearch.typing import TokenAddress

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
    last_known_chain_event_id = await storage.get_latest_chain_event_id()

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
    transfers, tip = await maybe_apply_tip(storage, tip_hash, transfers, last_affected_block, empty=[])

    url = request.app.router['token_transfers'].url_for(address=address)
    page = get_page(url=url, items=transfers, limit=limit, ordering=order)

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
@use_kwargs(TokenHoldersListSchema())
async def get_token_holders(
        request,
        address: TokenAddress,
        limit: int,
        order: Ordering,
        tip_hash: Optional[str] = None,
        balance: Optional[int] = None,
        _id: Optional[int] = None
):
    storage = request.app['storage']
    last_known_chain_event_id = await storage.get_latest_chain_event_id()

    # Notes: we need to query limit + 1 items to get link on next page
    holders, last_affected_block = await storage.get_tokens_holders(
        token_address=address,
        limit=limit + 1,
        ordering=order,
        balance=balance,
        _id=_id
    )

    holders, tip = await maybe_apply_tip(storage, tip_hash, holders, last_affected_block, empty=[])

    url = request.app.router['token_holders'].url_for(address=address)
    page = get_page(url=url, items=holders, limit=limit, ordering=order, decimals_to_ints=True)

    orphaned_request = await maybe_orphan_request(
        request,
        last_known_chain_event_id,
        last_affected_block,
        tip and tip.last_number,
    )

    if orphaned_request is not None:
        return orphaned_request

    return api_success(data=[x.to_dict() for x in page.items], page=page, meta=tip and tip.to_dict())
