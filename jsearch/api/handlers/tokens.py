import logging

from jsearch.api.helpers import validate_params, api_success

logger = logging.getLogger(__name__)


async def get_token_transfers(request):
    storage = request.app['storage']
    params = validate_params(request)
    contract_address = request.match_info['address'].lower()

    transfers, last_affected_block = await storage.get_tokens_transfers(
        address=contract_address,
        limit=params['limit'],
        offset=params['offset'],
        order=params['order']
    )
    return api_success([transfer.to_dict() for transfer in transfers])


async def get_token_holders(request):
    storage = request.app['storage']
    params = validate_params(request)
    token_address = request.match_info['address'].lower()

    holders, last_affected_block = await storage.get_tokens_holders(
        address=token_address,
        limit=params['limit'],
        offset=params['offset'],
        order=params['order']
    )
    return api_success([holder.to_dict() for holder in holders])
