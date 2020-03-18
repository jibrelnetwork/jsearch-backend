import logging
from typing import Dict, Any

import pytest

from jsearch.api.tests.utils import assert_not_404_response
from jsearch.tests.plugins.databases.factories.common import generate_address
from jsearch.tests.plugins.databases.factories.token_holder import TokenHolderFactory

logger = logging.getLogger(__name__)


@pytest.mark.usefixtures('main_db_data')
async def test_get_account_token_balance(cli):
    resp = await cli.get(f'/v1/accounts/a1/token_balance/t1')
    assert resp.status == 200
    res = (await resp.json())['data']
    assert res == {'accountAddress': 'a1', 'decimals': 2, 'balance': '1000', 'contractAddress': 't1'}

    resp = await cli.get(f'/v1/accounts/a3/token_balance/t3')
    assert resp.status == 200
    res = (await resp.json())['data']
    assert res == {'accountAddress': 'a3', 'decimals': 2, 'balance': '5000', 'contractAddress': 't3'}

    resp = await cli.get(f'/v1/accounts/a3/token_balance/tX')
    await assert_not_404_response(resp)

    resp = await cli.get(f'/v1/accounts/aX/token_balance/t1')
    await assert_not_404_response(resp)


async def test_get_account_balance_from_history(cli, token_holder_factory: TokenHolderFactory):
    # given
    account = generate_address()
    token = generate_address()

    data = {
        'account_address': account,
        'token_address': token,
    }

    # create history
    legacy_balance = token_holder_factory.create(**data)
    current_balance = token_holder_factory.create(**{**data, 'block_number': legacy_balance.block_number + 1})

    assert legacy_balance.balance != current_balance.balance

    # when
    resp = await cli.get(f'/v1/accounts/{account}/token_balance/{token}')
    resp_json = await resp.json()

    # then
    assert resp.status == 200
    assert resp_json['data']['balance'] == str(int(current_balance.balance))


async def test_get_account_balances_from_history(cli, token_holder_factory: TokenHolderFactory):
    # given
    account = generate_address()

    # first token
    token = generate_address()

    data = {
        'account_address': account,
        'token_address': token,
    }

    # balances
    first_legacy_balance = token_holder_factory.create(**data)
    first_current_balance = token_holder_factory.create(**{
        **data,
        'block_number': first_legacy_balance.block_number + 1
    })

    # second token
    second_token = generate_address()

    data = {
        'account_address': account,
        'token_address': second_token,
    }

    second_origin_balance = token_holder_factory.create(**data)
    second_current_balance = token_holder_factory.create(**{
        **data,
        'block_number': second_origin_balance.block_number + 1
    })

    # when
    resp = await cli.get(
        f'/v1/accounts/{account}/token_balances?contract_addresses={token},{second_token}'
    )
    resp_json = await resp.json()

    # then
    assert resp.status == 200

    received_data = resp_json['data']
    expected_data = [
        {
            'contractAddress': token,
            'balance': f'{int(first_current_balance.balance)}',
            'decimals': first_current_balance.decimals
        },
        {
            'contractAddress': second_token,
            'balance': f'{int(second_current_balance.balance)}',
            'decimals': second_current_balance.decimals
        }
    ]

    def key(item: Dict[str, Any]) -> bool:
        return item['contractAddress']

    assert sorted(received_data, key=key) == sorted(expected_data, key=key)
