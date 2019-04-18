import pytest

from aiohttp import web

from jsearch.api.app import make_app

pytest_plugins = (
    'jsearch.tests.plugins.settings',
)


@pytest.fixture
@pytest.mark.asyncio
async def node_server(aiohttp_server):
    async def handler(request):
        return web.json_response(data={'result': {'userAgent': request.headers['User-Agent']}})

    app = web.Application()
    app.router.add_route('*', '/{tail:.*}', handler)
    server = await aiohttp_server(app)

    yield server

    await server.close()


@pytest.fixture
@pytest.mark.asyncio
async def cli_with_node(event_loop, aiohttp_client, node_server, override_settings):
    override_settings('ETH_NODE_URL', str(node_server._root))

    app = await make_app()
    return await aiohttp_client(app)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('method', 'path'),
    (
        ('get', '/v1/gas_price'),
        ('post', '/v1/transaction_count'),
        ('post', '/v1/estimate_gas'),
        ('post', '/v1/call_contract'),
        ('post', '/v1/send_raw_transaction'),
    )
)
async def test_node_via_proxy_recieves_correct_user_agent(method, path, override_settings, node_server, cli_with_node):
    override_settings('HTTP_USER_AGENT', 'jsearch-backend/test hostname')

    resp = await cli_with_node.request(method, path, json=dict())
    resp_json = await resp.json()

    assert resp_json['data']['userAgent'] == 'jsearch-backend/test hostname'
