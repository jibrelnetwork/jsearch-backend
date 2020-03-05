from typing import List, Callable, Any, Dict

import pytest
import yarl
from aiohttp.test_utils import TestClient

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.api,
    pytest.mark.dex,
    pytest.mark.filtration
]


async def test_filtration_by_user_address(
        cli: TestClient,
        url: yarl.URL,
        user_address: str,
        random_assets_locks_and_unlocks: Callable[..., List[Dict[str, Any]]],
        get_total_locks: Callable[[List[Dict[str, Any]]], Dict[str, str]]
) -> None:
    # given

    # create events witch will filtered
    random_assets_locks_and_unlocks(user='0xtrash')

    # create expected events
    asset_events = random_assets_locks_and_unlocks(user=user_address)
    total_locks = get_total_locks(asset_events)

    # when
    response = await cli.get(str(url))
    resp_json = await response.json()

    # then
    assert response.status == 200
    assert resp_json['status']['success']

    assert len(resp_json['data']) > 0

    result = {(item['asset_address'], item['blocked_amount']) for item in resp_json['data']}
    expected = {(key, value) for key, value in total_locks.items()}

    assert result == expected


@pytest.mark.parametrize(
    'tokens, user_tokens, should_be_equal',
    [
        ('0xtoken', '0xtoken', True),
        ('0xtoken', None, False),
        ('0xtoken', '0xtrash', False),
        ('0xtoken, 0xasset', '0xtoken', True),
        ('0xtoken, 0xasset', '0xtoken, 0xasset', True),
    ]
)
async def test_filtration_by_token_address(
        cli: TestClient,
        url: yarl.URL,
        user_address: str,
        tokens: str,
        user_tokens: str,
        should_be_equal: bool,
        random_assets_locks_and_unlocks: Callable[..., List[Dict[str, Any]]],
        get_total_locks: Callable[[List[Dict[str, Any]]], Dict[str, str]]
) -> None:
    # create events
    total_locks = {}
    if user_tokens:
        user_tokens = user_tokens.split(', ')
        asset_events = random_assets_locks_and_unlocks(user=user_address, tokens=user_tokens)

        total_locks = get_total_locks(asset_events)

    url = url.with_query({'token_address': tokens})

    # when
    response = await cli.get(str(url))
    resp_json = await response.json()

    # then
    assert response.status == 200
    assert resp_json['status']['success']

    result = {(item['asset_address'], item['blocked_amount']) for item in resp_json['data']}
    expected = {(key, value) for key, value in total_locks.items()}

    assert should_be_equal and (result == expected) or not should_be_equal
