import logging

from jsearch.api.blockchain_tip import maybe_apply_tip
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

    transfers, tip_meta = await maybe_apply_tip(storage, tip_hash, transfers, last_affected_block, empty=[])
    transfers = [t.to_dict() for t in transfers]

    return api_success(transfers, meta=tip_meta)


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

    holders, tip_meta = await maybe_apply_tip(storage, tip_hash, holders, last_affected_block, empty=[])
    holders = [h.to_dict() for h in holders]

    return api_success(holders, meta=tip_meta)
