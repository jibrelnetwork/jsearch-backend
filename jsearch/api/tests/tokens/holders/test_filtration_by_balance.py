from urllib.parse import urlencode

import pytest
import yarl
from aiohttp.test_utils import TestClient

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.api,
    pytest.mark.token_holders,
    pytest.mark.filtration
]


@pytest.mark.parametrize(
    "parameter, value, status",
    (
            ('id', 2 ** 8, 200),
            ('balance', 2 ** 128, 200),
            ('id', 2 ** 128, 400),
    ),
    ids=(
            "id-low",
            "balance-enormous",
            "id-enormous",
    )
)
async def test_filter_by_balance(
        cli: TestClient,
        parameter: str,
        value: int,
        status: int,
        url: yarl.URL
):
    # given
    params = urlencode({parameter: value})
    url = url.with_query(params)

    # when
    resp = await cli.get(str(url))

    # then
    assert status == resp.status
