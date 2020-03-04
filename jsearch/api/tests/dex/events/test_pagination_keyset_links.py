from typing import Callable

import pytest
import yarl
from aiohttp.test_utils import TestClient

pytestmark = [pytest.mark.asyncio, pytest.mark.api, pytest.mark.dex, pytest.mark.pagination]


@pytest.mark.parametrize("key", ['next', 'link'])
async def test_keyset_links(
        cli: TestClient,
        url: yarl.URL,
        random_events: Callable[[int], None],
        key: str
) -> None:
    # given
    random_events(30)

    # when
    response = await cli.get(str(url))
    resp_json = await response.json()

    # then
    assert response.status == 200
    assert resp_json['status']['success']

    pagination = resp_json['paging']

    assert f'{key}' in pagination
    assert f'{key}_kwargs' in pagination

    assert str(url.with_query(pagination[f'{key}_kwargs'])) == resp_json['paging'][key]
