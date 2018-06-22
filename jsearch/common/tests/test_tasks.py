from unittest import mock

import pytest
from sqlalchemy import select

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


def test_process_new_verified_contract_transactions():
    pass