import pytest
from sqlalchemy import and_


@pytest.fixture()
def w3_contract_mock(mocker):
    W3 = mocker.patch('jsearch.common.processing.operations.Web3')
    return W3().eth.contract()


def test_update_contract_info(db, main_db_data, w3_contract_mock, mocker):
    w3_contract_mock.functions.name().call.return_value = 'xToken'
    w3_contract_mock.functions.symbol().call.return_value = 'XTK'
    w3_contract_mock.functions.decimals().call.return_value = 3
    w3_contract_mock.functions.totalSupply().call.return_value = 300

    patch_contract = mocker.patch('jsearch.common.processing.operations.patch_contract')

    from jsearch.common.processing.operations import update_token_info

    address = main_db_data['accounts'][2]['address']

    # when
    update_token_info(address)

    # then
    patch_contract.assert_called_with(
        '0xbb4af59aeaf2e83684567982af5ca21e9ac8419a',
        {'token_name': 'xToken', 'token_symbol': 'XTK', 'token_decimals': 3, 'token_total_supply': 300}
    )


def test_update_token_holder_balance(w3_contract_mock, db, db_connection_string, main_db_data):
    from jsearch.common import tables as t
    from jsearch.common.processing.operations import update_token_holder_balance

    from jsearch.common.database import MainDBSync

    token_address = main_db_data['accounts'][2]['address']
    account_address = main_db_data['accounts'][0]['address']

    w3_contract_mock.functions.balanceOf().call.return_value = 100500
    w3_contract_mock.functions.decimals().call.return_value = 2

    # when
    with MainDBSync(db_connection_string) as db_wrapper:
        update_token_holder_balance(db_wrapper, token_address, account_address, 3)

    # then
    q = t.token_holders_t.select().where(
        and_(t.token_holders_t.c.token_address == token_address,
             t.token_holders_t.c.account_address == account_address)
    )

    rows = db.execute(q).fetchall()
    assert len(rows) == 1
    assert rows[0].token_address == token_address
    assert rows[0].account_address == account_address
    assert rows[0].balance == 1005

    # when
    with MainDBSync(db_connection_string) as db_wrapper:
        w3_contract_mock.functions.balanceOf().call.return_value = 900
        update_token_holder_balance(db_wrapper, token_address, account_address, 3)

    # then
    rows = db.execute(q).fetchall()
    assert len(rows) == 1
    assert rows[0].token_address == token_address
    assert rows[0].account_address == account_address
    assert rows[0].balance == 9

    db.execute(t.token_holders_t.delete())
