from unittest.mock import ANY, call

from jsearch.common.tasks import on_new_contracts_added_task


def test_on_new_contracts_added_task(mocker, db):
    update_token_mock = mocker.patch(
        'jsearch.common.operations.update_token_info')
    process_transfer_mock = mocker.patch(
        'jsearch.common.processing.transactions.process_token_transfers_for_transaction')

    db.execute('INSERT INTO transactions (block_number, block_hash,  hash, transaction_index, "from", "to")'
               'VALUES (%s, %s, %s, %s, %s, %s)', [
                   (1, 'b1', 'a', 1, '0x1', '0x2'),
                   (2, 'b2', 'b', 1, '0x2', '0x3'),
                   (2, 'b2', 'c', 2, '0x3', '0x4'),
                   (2, 'b2', 'd', 3, '0x5', '0x3'),
               ])
    on_new_contracts_added_task('0x3', 'ABI')

    update_token_mock.assert_called_with('0x3', 'ABI')
    assert process_transfer_mock.call_count == 2
    process_transfer_mock.asert_has_calls([
        call(ANY, 'b'),
        call(ANY, 'd'),
    ])
