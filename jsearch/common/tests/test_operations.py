import pytest


@pytest.fixture()
def w3_contract_mock(mocker):
    W3 = mocker.patch('jsearch.common.operations.Web3')
    return W3().eth.contract()


def test_update_contract_info(db, main_db_data, w3_contract_mock, mocker):
    w3_contract_mock.functions.name().call.return_value = 'xToken'
    w3_contract_mock.functions.symbol().call.return_value = 'XTK'
    w3_contract_mock.functions.decimals().call.return_value = 3
    w3_contract_mock.functions.totalSupply().call.return_value = 300

    patch_contract = mocker.patch('jsearch.common.operations.patch_contract')

    from jsearch.common.operations import update_token_info

    address = main_db_data['accounts'][2]['address']

    # when
    update_token_info(address)

    # then
    patch_contract.assert_called_with(
        '0xbb4af59aeaf2e83684567982af5ca21e9ac8419a',
        {'token_name': 'xToken', 'token_symbol': 'XTK', 'token_decimals': 3, 'token_total_supply': 300}
    )
