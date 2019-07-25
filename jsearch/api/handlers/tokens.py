import logging

from jsearch.api.blockchain_tip import get_tip_or_raise_api_error, is_tip_stale
from jsearch.api.helpers import validate_params, api_success

logger = logging.getLogger(__name__)


async def get_token_transfers(request):
    storage = request.app['storage']
    params = validate_params(request)
    contract_address = request.match_info['address'].lower()
    tip_hash = request.query.get('blockchain_tip') or None

    transfers, last_affected_block = await storage.get_tokens_transfers(
        address=contract_address,
        limit=params['limit'],
        offset=params['offset'],
        order=params['order']
    )

    tip = tip_hash and await get_tip_or_raise_api_error(storage, tip_hash)
    tip_is_stale = is_tip_stale(tip, last_affected_block)

    transfers = [] if tip_is_stale else transfers
    transfers = [t.to_dict() for t in transfers]

    return api_success(transfers)


async def get_token_holders(request):
    storage = request.app['storage']
    params = validate_params(request)
    token_address = request.match_info['address'].lower()
    tip_hash = request.query.get('blockchain_tip') or None

    holders, last_affected_block = await storage.get_tokens_holders(
        address=token_address,
        limit=params['limit'],
        offset=params['offset'],
        order=params['order']
    )

    tip = tip_hash and await get_tip_or_raise_api_error(storage, tip_hash)
    tip_is_stale = is_tip_stale(tip, last_affected_block)

    holders = [] if tip_is_stale else holders
    holders = [h.to_dict() for h in holders]

    return api_success(holders)
