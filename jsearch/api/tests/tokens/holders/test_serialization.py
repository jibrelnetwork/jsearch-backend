from typing import Callable, Any

import pytest
import yarl
from aiohttp.test_utils import TestClient

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.api,
    pytest.mark.token_holders,
    pytest.mark.serialization
]


async def test_serialization(
        cli: TestClient,
        token_address: str,
        token_holder_factory: Callable[..., Any]
) -> None:
    # given
    data = {
        'account_address': '0xa3dress',
        'decimals': 2,
        'balance': 3000,
        'token_address': token_address
    }
    transfer = token_holder_factory.create(**data)

    # when
    resp = await cli.get(f'/v1/tokens/{transfer.token_address}/holders')
    resp_json = await resp.json()

    # then
    assert resp.status == 200
    assert resp_json['data'] == [
        {
            'accountAddress': '0xa3dress',
            'decimals': 2,
            'balance': 3000,
            'contractAddress': token_address,
            'id': transfer.id
        },
    ]


async def test_serialization_enormous_balance(
        cli: TestClient,
        url: yarl.URL,
        token_address: str,
        create_token_holders: Callable[..., Any]
) -> None:
    # See: JSEARCH-499.

    # given
    create_token_holders(token_address, balances=(1000000000000000000,))
    url = url.with_query({'limit': 1})

    # when
    resp = await cli.get(str(url))
    resp_json = await resp.json()

    # then
    assert resp_json['paging'] == {
        'link': f'/v1/tokens/{token_address}/holders?'
                f'balance=1000000000000000000&'
                f'id=1&'
                f'order=desc&'
                f'limit=1',
        'link_kwargs': {
            'balance': '1000000000000000000',
            'id': '1',
            'order': 'desc',
            'limit': '1'
        },
        'next': f'/v1/tokens/{token_address}/holders?'
                f'balance=1000000000000000000&'
                f'id=0&'
                f'order=desc&'
                f'limit=1',
        'next_kwargs': {
            'balance': '1000000000000000000',
            'id': '0',
            'order': 'desc',
            'limit': '1'
        }
    }
