from unittest import mock

import pytest
from sqlalchemy import select, and_

from jsearch.common import tasks
from jsearch.common import tables as t


@mock.patch('jsearch.common.tasks.Web3')
def test_update_contract_info(W3, db, contracts, main_db_data):
    address = main_db_data['accounts'][2]['address']
    m = W3().eth.contract()
    m.functions.name().call.return_value = 'xToken'
    m.functions.symbol().call.return_value = 'XTK'
    m.functions.decimals().call.return_value = 3
    m.functions.totalSupply().call.return_value = 300

    tasks.update_token_info(address)

    q = select([t.contracts_t]).where(t.contracts_t.c.address == address)
    rows = db.execute(q).fetchall()

    assert rows[0].token_name == 'xToken'
    assert rows[0].token_symbol == 'XTK'
    assert rows[0].token_decimals == 3
    assert rows[0].token_total_supply == 300


@mock.patch('jsearch.common.tasks.Web3')
def test_update_token_holder_balance(W3, db, contracts, main_db_data):
    token_address = main_db_data['accounts'][2]['address']
    account_address = main_db_data['accounts'][0]['address']
    m = W3().eth.contract()
    m.functions.balanceOf().call.return_value = 100500
    m.functions.decimals().call.return_value = 2
    tasks.update_token_holder_balance(token_address, account_address)

    q = t.token_holders_t.select().where(and_(t.token_holders_t.c.token_address == token_address,
                                              t.token_holders_t.c.account_address == account_address))
    rows = db.execute(q).fetchall()
    assert len(rows) == 1
    assert rows[0].token_address == token_address
    assert rows[0].account_address == account_address
    assert rows[0].balance == 1005

    m.functions.balanceOf().call.return_value = 900
    tasks.update_token_holder_balance(token_address, account_address)
    rows = db.execute(q).fetchall()
    assert len(rows) == 1
    assert rows[0].token_address == token_address
    assert rows[0].account_address == account_address
    assert rows[0].balance == 9

    db.execute(t.token_holders_t.delete())