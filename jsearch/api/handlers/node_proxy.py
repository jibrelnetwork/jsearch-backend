import logging

from jsearch.api.helpers import proxy_response

logger = logging.getLogger(__name__)


async def get_gas_price(request):
    resp = await request.app['node_proxy'].gas_price()
    return proxy_response(resp)


async def get_transaction_count(request):
    args = await request.json()
    resp = await request.app['node_proxy'].transaction_count(args)
    return proxy_response(resp)


async def calculate_estimate_gas(request):
    args = await request.json()
    resp = await request.app['node_proxy'].estimate_gas(args)
    return proxy_response(resp)


async def call_contract(request):
    args = await request.json()
    resp = await request.app['node_proxy'].call_contract(args)
    return proxy_response(resp)


async def send_raw_transaction(request):
    args = await request.json()
    resp = await request.app['node_proxy'].send_raw_transaction(args)
    return proxy_response(resp)
