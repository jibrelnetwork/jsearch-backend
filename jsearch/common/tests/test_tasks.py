from jsearch.common.database import MainDBSync
from jsearch.common.tasks import on_new_contracts_added_task


def test_reset_logs_after_on_new_contracts_added_task(db, db_connection_string):
    # given We have some processed logs
    db.execute(
        'INSERT INTO logs ("transaction_hash", "block_number", "block_hash", "log_index", "address", "is_processed") '
        'VALUES (%s, %s, %s, %s, %s, %s)',
        [
            ('sd', 1, 'hash_1', 2, '0x3', True),
            ('sw', 1, 'hash_1', 3, '0x3', True),
            ('xw', 2, 'hash_2', 1, '0x4', True),
            ('we', 2, 'hash_2', 2, '0x4', True),
        ]
    )

    # when We run task
    on_new_contracts_added_task('0x3')

    # then We expected what logs what only half logs with same address will be reset
    with MainDBSync(db_connection_string) as main_db:
        logs = main_db.get_logs_to_process_events()

    assert sorted(log['address'] for log in logs) == ['0x3', '0x3']
