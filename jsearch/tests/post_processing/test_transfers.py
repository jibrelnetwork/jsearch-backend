from decimal import Decimal


def test_log_to_transfer():
    from jsearch.common.processing.erc20_balances import log_to_transfers
    log = {
        'address': '0x7cbc8ee27ffdba230dd316160ea01d565f17aacb',
        'block_hash': '0xa6c837fb9d5495872238e141e3b4a4d71dc34218a09d3b7eee2beebdad02d7b7',
        'block_number': 6647925,
        'data': '0x00000000000000000000000000000000000000000000000c5c22b80115100000',
        'event_args': {
            'from': '0xfac652fb819674245a96c264f0a79e9157533347',
            'to': '0x18134528f87f786c7b30e9a20fa3d9797eaa0776',
            'value': 228000000000000000000
        },
        'event_type': 'Transfer',
        'is_forked': False,
        'is_processed': True,
        'is_token_transfer': True,
        'is_transfer_processed': True,
        'log_index': 29,
        'removed': False,
        'token_amount': Decimal('228000000000000000000'),
        'token_transfer_from': '0xfac652fb819674245a96c264f0a79e9157533347',
        'token_transfer_to': '0x18134528f87f786c7b30e9a20fa3d9797eaa0776',
        'topics': [
            '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef',
            '0x000000000000000000000000fac652fb819674245a96c264f0a79e9157533347',
            '0x00000000000000000000000018134528f87f786c7b30e9a20fa3d9797eaa0776'
        ],
        'transaction_hash': '0x5ffd0917e263c98e7fcacb9b87aff5f584b40b2015c18afd7a7ed8a06bbc05f1',
        'transaction_index': 114
    }
    block = {
        'difficulty': 3033008076446112,
        'extra_data': '0xe4b883e5bda9e7a59ee4bb99e9b1bc',
        'gas_limit': 8000000,
        'gas_used': 0,
        'hash': '0xa6c837fb9d5495872238e141e3b4a4d71dc34218a09d3b7eee2beebdad02d7b7',
        'is_forked': False,
        'is_sequence_sync': False,
        'logs_bloom': '0x00000000000000000000000000000000000000000000000000000000000000'
                      '0000000000000000000000000000000000000000000000000000000000000000'
                      '0000000000000000000000000000000000000000000000000000000000000000'
                      '0000000000000000000000000000000000000000000000000000000000000000'
                      '0000000000000000000000000000000000000000000000000000000000000000'
                      '0000000000000000000000000000000000000000000000000000000000000000'
                      '0000000000000000000000000000000000000000000000000000000000000000'
                      '000000000000000000000000000000000000000000000000000000000000000000',
        'miner': '0x829bd824b016326a401d083b33d092293333a830',
        'mix_hash': '0x1fdbd77f2227df6971a5f9a6bde520a5830b28ff136d417c60554341767b9e95',
        'nonce': '0x3b94fa7800749e04',
        'number': 6648009,
        'parent_hash': '0xe979d29ac0ff94de6d9e5fa9580e58683aa888336d2d2caae8d43f09be7d0c1b',
        'receipts_root': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421',
        'sha3_uncles': '0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347',
        'size': None,
        'state_root': '0x6d3d6cd14daae229847e623594fb30abeee61ff0c00ee70adefafe03526bb1aa',
        'static_reward': Decimal('3000000000000000000'),
        'timestamp': 1541421079,
        'total_difficulty': None,
        'transactions_root': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421',
        'tx_fees': Decimal('0'),
        'uncle_inclusion_reward': Decimal('0')

    }
    contract = {
        'address': '0xdfb410994b66778bd6cc2c82e8ffe4f7b2870006',
        'token_symbol': 'ICAP',
        'token_name': 'Integrated Capital Token',
        'decimals': 18
    }

    # when
    transfers = log_to_transfers(log, block, contract)

    # then
    assert transfers == [
        {
            'address': '0x18134528f87f786c7b30e9a20fa3d9797eaa0776',
            'block_hash': '0xa6c837fb9d5495872238e141e3b4a4d71dc34218a09d3b7eee2beebdad02d7b7',
            'block_number': 6647925,
            'from_address': '0xfac652fb819674245a96c264f0a79e9157533347',
            'log_index': 29,
            'to_address': '0x18134528f87f786c7b30e9a20fa3d9797eaa0776',
            'timestamp': 1541421079,
            'token_address': '0x7cbc8ee27ffdba230dd316160ea01d565f17aacb',
            'token_decimals': 18,
            'token_name': 'Integrated Capital Token',
            'token_symbol': 'ICAP',
            'token_value': Decimal('228000000000000000000'),
            'transaction_hash': '0x5ffd0917e263c98e7fcacb9b87aff5f584b40b2015c18afd7a7ed8a06bbc05f1'
        },
        {
            'address': '0xfac652fb819674245a96c264f0a79e9157533347',
            'block_hash': '0xa6c837fb9d5495872238e141e3b4a4d71dc34218a09d3b7eee2beebdad02d7b7',
            'block_number': 6647925,
            'from_address': '0xfac652fb819674245a96c264f0a79e9157533347',
            'log_index': 29,
            'to_address': '0x18134528f87f786c7b30e9a20fa3d9797eaa0776',
            'timestamp': 1541421079,
            'token_address': '0x7cbc8ee27ffdba230dd316160ea01d565f17aacb',
            'token_decimals': 18,
            'token_name': 'Integrated Capital Token',
            'token_symbol': 'ICAP',
            'token_value': Decimal('228000000000000000000'),
            'transaction_hash': '0x5ffd0917e263c98e7fcacb9b87aff5f584b40b2015c18afd7a7ed8a06bbc05f1'
        }
    ]


def test_logs_to_transfer():
    from jsearch.common.processing.erc20_balances import logs_to_transfers
    logs = [
        {
            'address': '0x7cbc8ee27ffdba230dd316160ea01d565f17aacb',
            'block_hash': '0xa6c837fb9d5495872238e141e3b4a4d71dc34218a09d3b7eee2beebdad02d7b7',
            'block_number': 6647925,
            'data': '0x00000000000000000000000000000000000000000000000c5c22b80115100000',
            'event_args': {
                'from': '0xfac652fb819674245a96c264f0a79e9157533347',
                'to': '0x18134528f87f786c7b30e9a20fa3d9797eaa0776',
                'value': 228000000000000000000
            },
            'event_type': 'Transfer',
            'is_forked': False,
            'is_processed': True,
            'is_token_transfer': True,
            'is_transfer_processed': True,
            'log_index': 29,
            'removed': False,
            'token_amount': Decimal(228000000000000000000),
            'token_transfer_from': '0xfac652fb819674245a96c264f0a79e9157533347',
            'token_transfer_to': '0x18134528f87f786c7b30e9a20fa3d9797eaa0776',
            'topics': [
                '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef',
                '0x000000000000000000000000fac652fb819674245a96c264f0a79e9157533347',
                '0x00000000000000000000000018134528f87f786c7b30e9a20fa3d9797eaa0776'
            ],
            'transaction_hash': '0x5ffd0917e263c98e7fcacb9b87aff5f584b40b2015c18afd7a7ed8a06bbc05f1',
            'transaction_index': 114
        },
        {
            'address': '0x7cbc8ee27ffdba230dd316160ea01d565f17aacb',
            'block_hash': '0xa6c837fb9d5495872238e141e3b4a4d71dc34218a09d3b7eee2beebdad02d7b7',
            'block_number': 6647925,
            'data': '0x0000000000000000000000000000000000000000000000000000000000000000',
            'event_args': {
                'from': '0x85429f986a5cc38f90de7b4ffa44d570eef04066',
                'to': '0x35047d681920f66e4ad32c6d6c2a7091fa15a209',
                'value': 2234000000000000000000
            },
            'event_type': 'Transfer',
            'is_forked': False,
            'is_processed': True,
            'is_token_transfer': True,
            'is_transfer_processed': True,
            'log_index': 40,
            'removed': False,
            'token_amount': Decimal(2234000000000000000000),
            'token_transfer_from': '0x85429f986a5cc38f90de7b4ffa44d570eef04066',
            'token_transfer_to': '0x35047d681920f66e4ad32c6d6c2a7091fa15a209',
            'topics': [
                '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef',
                '0x00000000000000000000000085429f986a5cc38f90de7b4ffa44d570eef04066',
                '0x00000000000000000000000035047d681920f66e4ad32c6d6c2a7091fa15a209'
            ],
            'transaction_hash': '0x90bfe01af4b1f7959b03dcb9b41a78525d481697fcce27fea4af767d57d37451',
            'transaction_index': 129
        }
    ]
    blocks = {
        '0xa6c837fb9d5495872238e141e3b4a4d71dc34218a09d3b7eee2beebdad02d7b7': {
            'difficulty': 3033008076446112,
            'extra_data': '0xe4b883e5bda9e7a59ee4bb99e9b1bc',
            'gas_limit': 8000000,
            'gas_used': 0,
            'hash': '0xa6c837fb9d5495872238e141e3b4a4d71dc34218a09d3b7eee2beebdad02d7b7',
            'is_forked': False,
            'is_sequence_sync': False,
            'logs_bloom': '0x00000000000000000000000000000000000000000000000000000000000000'
                          '0000000000000000000000000000000000000000000000000000000000000000'
                          '0000000000000000000000000000000000000000000000000000000000000000'
                          '0000000000000000000000000000000000000000000000000000000000000000'
                          '0000000000000000000000000000000000000000000000000000000000000000'
                          '0000000000000000000000000000000000000000000000000000000000000000'
                          '0000000000000000000000000000000000000000000000000000000000000000'
                          '000000000000000000000000000000000000000000000000000000000000000000',
            'miner': '0x829bd824b016326a401d083b33d092293333a830',
            'mix_hash': '0x1fdbd77f2227df6971a5f9a6bde520a5830b28ff136d417c60554341767b9e95',
            'nonce': '0x3b94fa7800749e04',
            'number': 6648009,
            'parent_hash': '0xe979d29ac0ff94de6d9e5fa9580e58683aa888336d2d2caae8d43f09be7d0c1b',
            'receipts_root': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421',
            'sha3_uncles': '0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347',
            'size': None,
            'state_root': '0x6d3d6cd14daae229847e623594fb30abeee61ff0c00ee70adefafe03526bb1aa',
            'static_reward': Decimal('3000000000000000000'),
            'timestamp': 1541421079,
            'total_difficulty': None,
            'transactions_root': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421',
            'tx_fees': Decimal('0'),
            'uncle_inclusion_reward': Decimal('0')

        }
    }
    contracts = {
        '0x7cbc8ee27ffdba230dd316160ea01d565f17aacb': {
            'address': '0x7cbc8ee27ffdba230dd316160ea01d565f17aacb',
            'token_symbol': 'ICAP',
            'token_name': 'Integrated Capital Token',
            'decimals': 18
        }
    }

    # when
    transfers = logs_to_transfers(logs, blocks, contracts)

    # then
    assert transfers == [
        {
            'address': '0x18134528f87f786c7b30e9a20fa3d9797eaa0776',
            'block_hash': '0xa6c837fb9d5495872238e141e3b4a4d71dc34218a09d3b7eee2beebdad02d7b7',
            'block_number': 6647925,
            'from_address': '0xfac652fb819674245a96c264f0a79e9157533347',
            'log_index': 29,
            'to_address': '0x18134528f87f786c7b30e9a20fa3d9797eaa0776',
            'timestamp': 1541421079,
            'token_address': '0x7cbc8ee27ffdba230dd316160ea01d565f17aacb',
            'token_decimals': 18,
            'token_name': 'Integrated Capital Token',
            'token_symbol': 'ICAP',
            'token_value': Decimal('228000000000000000000'),
            'transaction_hash': '0x5ffd0917e263c98e7fcacb9b87aff5f584b40b2015c18afd7a7ed8a06bbc05f1'
        },
        {
            'address': '0xfac652fb819674245a96c264f0a79e9157533347',
            'block_hash': '0xa6c837fb9d5495872238e141e3b4a4d71dc34218a09d3b7eee2beebdad02d7b7',
            'block_number': 6647925,
            'from_address': '0xfac652fb819674245a96c264f0a79e9157533347',
            'log_index': 29,
            'to_address': '0x18134528f87f786c7b30e9a20fa3d9797eaa0776',
            'timestamp': 1541421079,
            'token_address': '0x7cbc8ee27ffdba230dd316160ea01d565f17aacb',
            'token_decimals': 18,
            'token_name': 'Integrated Capital Token',
            'token_symbol': 'ICAP',
            'token_value': Decimal('228000000000000000000'),
            'transaction_hash': '0x5ffd0917e263c98e7fcacb9b87aff5f584b40b2015c18afd7a7ed8a06bbc05f1'
        },
        {
            'address': '0x35047d681920f66e4ad32c6d6c2a7091fa15a209',
            'block_hash': '0xa6c837fb9d5495872238e141e3b4a4d71dc34218a09d3b7eee2beebdad02d7b7',
            'block_number': 6647925,
            'from_address': '0x85429f986a5cc38f90de7b4ffa44d570eef04066',
            'log_index': 40,
            'to_address': '0x35047d681920f66e4ad32c6d6c2a7091fa15a209',
            'timestamp': 1541421079,
            'token_address': '0x7cbc8ee27ffdba230dd316160ea01d565f17aacb',
            'token_decimals': 18,
            'token_name': 'Integrated Capital Token',
            'token_symbol': 'ICAP',
            'token_value': Decimal('2234000000000000000000'),
            'transaction_hash': '0x90bfe01af4b1f7959b03dcb9b41a78525d481697fcce27fea4af767d57d37451'
        },
        {
            'address': '0x85429f986a5cc38f90de7b4ffa44d570eef04066',
            'block_hash': '0xa6c837fb9d5495872238e141e3b4a4d71dc34218a09d3b7eee2beebdad02d7b7',
            'block_number': 6647925,
            'from_address': '0x85429f986a5cc38f90de7b4ffa44d570eef04066',
            'log_index': 40,
            'to_address': '0x35047d681920f66e4ad32c6d6c2a7091fa15a209',
            'timestamp': 1541421079,
            'token_address': '0x7cbc8ee27ffdba230dd316160ea01d565f17aacb',
            'token_decimals': 18,
            'token_name': 'Integrated Capital Token',
            'token_symbol': 'ICAP',
            'token_value': Decimal('2234000000000000000000'),
            'transaction_hash': '0x90bfe01af4b1f7959b03dcb9b41a78525d481697fcce27fea4af767d57d37451'
        }]


def test_insert_transfers_to_db(db, db_connection_string, mocker):
    mocker.patch('time.sleep')
    from jsearch.common.database import MainDBSync

    # given
    transfers = [
        {
            'address': '0x18134528f87f786c7b30e9a20fa3d9797eaa0776',
            'block_hash': '0xa6c837fb9d5495872238e141e3b4a4d71dc34218a09d3b7eee2beebdad02d7b7',
            'block_number': 6647925,
            'from_address': '0xfac652fb819674245a96c264f0a79e9157533347',
            'log_index': 29,
            'to_address': '0x18134528f87f786c7b30e9a20fa3d9797eaa0776',
            'timestamp': 1541421079,
            'token_address': '0x7cbc8ee27ffdba230dd316160ea01d565f17aacb',
            'token_decimals': 18,
            'token_name': 'Integrated Capital Token',
            'token_symbol': 'ICAP',
            'token_value': Decimal('228.0'),
            'transaction_hash': '0x5ffd0917e263c98e7fcacb9b87aff5f584b40b2015c18afd7a7ed8a06bbc05f1'
        },
        {
            'address': '0xfac652fb819674245a96c264f0a79e9157533347',
            'block_hash': '0xa6c837fb9d5495872238e141e3b4a4d71dc34218a09d3b7eee2beebdad02d7b7',
            'block_number': 6647925,
            'from_address': '0xfac652fb819674245a96c264f0a79e9157533347',
            'log_index': 29,
            'to_address': '0x18134528f87f786c7b30e9a20fa3d9797eaa0776',
            'timestamp': 1541421079,
            'token_address': '0x7cbc8ee27ffdba230dd316160ea01d565f17aacb',
            'token_decimals': 18,
            'token_name': 'Integrated Capital Token',
            'token_symbol': 'ICAP',
            'token_value': Decimal('228.0'),
            'transaction_hash': '0x5ffd0917e263c98e7fcacb9b87aff5f584b40b2015c18afd7a7ed8a06bbc05f1'
        }
    ]

    # when
    with MainDBSync(db_connection_string) as main_db:
        main_db.insert_transfers(transfers)

    # then
    result = db.execute('SELECT count(*) FROM token_transfers').fetchone()
    assert result['count'] == 2
