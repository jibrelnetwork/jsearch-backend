import pytest
from aiohttp.test_utils import TestClient

pytestmark = pytest.mark.usefixtures('disable_metrics_setup')


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

    assert (resp.status, resp_json['status']['errors']) == (
        400,
        [
            {
                'field': 'non_field_error',
                'error_code': 'INVALID_BODY',
                'error_message': 'The provided body is not a valid JSON.'
            }
        ]
    )
