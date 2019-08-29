import logging

import pytest
from typing import Dict, Any

from jsearch import settings
from jsearch.api.tests.utils import assert_not_404_response
from jsearch.tests.plugins.databases.factories.common import generate_address
from jsearch.tests.plugins.databases.factories.token_holder import TokenHolderFactory

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.usefixtures('disable_metrics_setup')


async def test_get_account_balances(cli, main_db_data):
    a1 = main_db_data['accounts_base'][0]
    a2 = main_db_data['accounts_base'][1]
    resp = await cli.get('/v1/accounts/balances?addresses={},{}'.format(a1['address'], a2['address']))
    assert resp.status == 200
    res = (await resp.json())['data']
    assert res == [{'address': a1['address'],
                    'balance': str(main_db_data['accounts_state'][10]['balance'])},
                   {'address': a2['address'],
                    'balance': str(main_db_data['accounts_state'][6]['balance'])}]


async def test_get_account_balance_from_history(cli, token_holder_factory: TokenHolderFactory):
    # given
    account = generate_address()
    token = generate_address()

    data = {
        'account_address': account,
        'token_address': token,
    }

    # balances
    legacy_balance = token_holder_factory.create(**data)
    current_balance = token_holder_factory.create(**{**data, 'block_number': legacy_balance.block_number + 1})

    # when
    resp = await cli.get(f'/v1/accounts/{account}/token_balance/{token}')
    resp_json = await resp.json()

    # then
    assert resp.status == 200
    assert resp_json['data'] == {
        'accountAddress': account,
        'contractAddress': token,
        'balance': f'{int(current_balance.balance)}',
        'decimals': current_balance.decimals
    }


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


async def test_get_account_balances_invalid_addresses_all(cli):
    resp = await cli.get('/v1/accounts/balances?addresses=foobar')
    assert resp.status == 200
    res = (await resp.json())['data']
    assert res == []


async def test_get_account_balances_addresses_have_spaces(cli, main_db_data):
    a1 = main_db_data['accounts_base'][0]
    a2 = main_db_data['accounts_base'][1]
    resp = await cli.get('/v1/accounts/balances?addresses={}, {}'.format(a1['address'], a2['address']))
    res = (await resp.json())['data']
    assert resp.status == 200
    assert len(res) == 2


async def test_get_account_balances_invalid_addresses(cli: object, main_db_data: object) -> object:
    a1 = main_db_data['accounts_base'][0]
    resp = await cli.get('/v1/accounts/balances?addresses={},{},{}'.format('foo', a1['address'], 'bar'))
    assert resp.status == 200
    res = (await resp.json())['data']
    assert res == [{'address': a1['address'],
                    'balance': str(main_db_data['accounts_state'][10]['balance'])}]


async def test_get_account_token_balance(cli, main_db_data):
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


async def test_get_accounts_balances_does_not_complain_on_addresses_count_less_than_limit(cli):
    addresses = [f'a{x}' for x in range(settings.API_QUERY_ARRAY_MAX_LENGTH)]
    addresses_str = ','.join(addresses)

    resp = await cli.get(f'/v1/accounts/balances?addresses={addresses_str}')

    assert resp.status == 200


async def test_get_accounts_balances_complains_on_addresses_count_more_than_limit(cli):
    addresses = [f'a{x}' for x in range(settings.API_QUERY_ARRAY_MAX_LENGTH + 1)]
    addresses_str = ','.join(addresses)

    resp = await cli.get(f'/v1/accounts/balances?addresses={addresses_str}')
    resp_json = await resp.json()

    assert resp.status == 400
    assert resp_json['status']['errors'] == [
        {
            'field': 'addresses',
            'error_code': 'TOO_MANY_ITEMS',
            'error_message': 'Too many addresses requested'
        }
    ]
