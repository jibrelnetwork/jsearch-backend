import pytest
from aiohttp.test_utils import TestClient
from pytest_mock import MockFixture

from jsearch.api.node_proxy import NodeProxy


@pytest.mark.parametrize(
    'endpoint',
    (
        '/v1/proxy/transaction_count',
        '/v1/proxy/estimate_gas',
        '/v1/proxy/call_contract',
        '/v1/proxy/send_raw_transaction',
    ),
)
async def test_invalid_json_for_proxy_endpoints_should_return_bad_request(
        cli: TestClient,
        endpoint: str,
) -> None:
    resp = await cli.post(endpoint, data="{'invalid', 'json'}")
    resp_json = await resp.json()

    assert (resp.status, resp_json) == (
        400,
        {
            'status': {
                'success': False,
                'errors': [
                    {
                        'field': 'non_field_error',
                        'code': 'INVALID_BODY',
                        'message': 'The provided body is not a valid JSON.'
                    }
                ]
            },
            'data': None,
        }
    )


@pytest.mark.parametrize(
    'method, endpoint',
    (
        ('GET', '/v1/proxy/gas_price'),
        ('POST', '/v1/proxy/transaction_count'),
        ('POST', '/v1/proxy/estimate_gas'),
        ('POST', '/v1/proxy/call_contract'),
        ('POST', '/v1/proxy/send_raw_transaction'),
    ),
)
async def test_valid_json_for_proxy_endpoints_should_return_node_response(
        cli: TestClient,
        mocker: MockFixture,
        method: str,
        endpoint: str,
) -> None:
    # Mocked response does not match the spec.
    cli.app['validate_spec'] = False

    async def _request(self, method, params=None):
        return {'result': ['response', 'from', 'the', 'node']}

    mocker.patch.object(NodeProxy, '_request', _request)

    resp = await cli.request(method, endpoint, data='{"valid": "json"}')
    resp_json = await resp.json()

    assert (resp.status, resp_json) == (
        200,
        {
            'status': {
                'success': True,
                'errors': []
            },
            'data': ['response', 'from', 'the', 'node'],
        }
    )


@pytest.mark.parametrize(
    'method, endpoint',
    (
        ('GET', '/v1/proxy/gas_price'),
        ('POST', '/v1/proxy/transaction_count'),
        ('POST', '/v1/proxy/estimate_gas'),
        ('POST', '/v1/proxy/call_contract'),
        ('POST', '/v1/proxy/send_raw_transaction'),
    ),
)
async def test_node_proxy_proxies_errors_from_the_node(
        cli: TestClient,
        mocker: MockFixture,
        method: str,
        endpoint: str,
) -> None:
    async def _request(self, method, params=None):
        return {
            "error": {
                "code": -32602,
                "message": (
                    "invalid argument 0: json: cannot unmarshal hex string "
                    "without 0x prefix into Go struct field CallArgs.gas of "
                    "type hexutil.Uint64"
                ),
            }
        }

    mocker.patch.object(NodeProxy, '_request', _request)

    resp = await cli.request(method, endpoint, data='{"valid": "json"}')
    resp_json = await resp.json()

    assert (resp.status, resp_json) == (
        200,
        {
            'status': {
                'success': False,
                'errors': [
                    {
                        "field": "non_field_error",
                        "code": -32602,
                        "message": (
                            "invalid argument 0: json: cannot unmarshal hex "
                            "string without 0x prefix into Go struct field "
                            "CallArgs.gas of type hexutil.Uint64"
                        ),
                    }
                ]
            },
            'data': None,
        }
    )
