import logging

from jsearch.api.helpers import proxy_response, load_json_or_raise_api_error, ApiError

logger = logging.getLogger(__name__)


async def get_gas_price(request):
    resp = await request.app['node_proxy'].gas_price()
    return proxy_response(resp)


@ApiError.catch
async def get_transaction_count(request):
    args = await load_json_or_raise_api_error(request)
    resp = await request.app['node_proxy'].transaction_count(args)
    return proxy_response(resp)


@ApiError.catch
async def calculate_estimate_gas(request):
    args = await load_json_or_raise_api_error(request)
    resp = await request.app['node_proxy'].estimate_gas(args)
    return proxy_response(resp)


@ApiError.catch
async def call_contract(request):
    args = await load_json_or_raise_api_error(request)
    resp = await request.app['node_proxy'].call_contract(args)
    return proxy_response(resp)


@ApiError.catch
async def send_raw_transaction(request):
    args = await load_json_or_raise_api_error(request)
    resp = await request.app['node_proxy'].send_raw_transaction(args)
    return proxy_response(resp)
