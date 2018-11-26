from typing import List
from unittest import mock

import pytest
from asynctest import CoroutineMock

from jsearch.tests.entities import (
    TransactionFromDumpWrapper,
    BlockFromDumpWrapper,
    AccountFromDumpWrapper,
    TokenTransferFromDumpWrapper,
    ReceiptFromDumpWrapper)
from jsearch.tests.utils import pprint_returned_value

pytest_plugins = [
    'jsearch.tests.plugins.tools',
    'jsearch.tests.plugins.databases.main_db',
    'jsearch.tests.plugins.databases.dumps',
]


async def test_get_block_404(cli):
    resp = await cli.get('/blocks/1')
    assert resp.status == 404


async def test_get_block_by_number(cli, main_db_data):
    # given
    txs = TransactionFromDumpWrapper.from_dump(
        main_db_data,
        filters={"block_number": 2},
        bulk=True
    )
    block = BlockFromDumpWrapper.from_dump(
        dump=main_db_data,
        filters={'number': 2},
        transactions=[tx.entity.hash for tx in txs]
    )
    # then
    resp = await cli.get('/blocks/2')
    assert resp.status == 200
    assert await resp.json() == block.as_dict()


async def test_get_block_by_hash(cli, main_db_data):
    resp = await cli.get('/blocks/' + main_db_data['blocks'][0]['hash'])
    assert resp.status == 200
    b = main_db_data['blocks'][0]
    assert await resp.json() == {
        'difficulty': b['difficulty'],
        'extraData': b['extra_data'],
        'gasLimit': b['gas_limit'],
        'gasUsed': b['gas_used'],
        'hash': b['hash'],
        'logsBloom': b['logs_bloom'],
        'miner': b['miner'],
        'mixHash': b['mix_hash'],
        'nonce': b['nonce'],
        'number': b['number'],
        'parentHash': b['parent_hash'],
        'receiptsRoot': b['receipts_root'],
        'sha3Uncles': b['sha3_uncles'],
        'size': b['size'],
        'stateRoot': b['state_root'],
        'timestamp': b['timestamp'],
        'totalDifficulty': b['total_difficulty'],
        'transactions': [],
        'transactionsRoot': b['transactions_root'],
        'staticReward': b['static_reward'],
        'txFees': b['tx_fees'],
        'uncleInclusionReward': b['uncle_inclusion_reward'],
        'uncles': None
    }


async def test_get_block_latest(cli, main_db_data):
    resp = await cli.get('/blocks/latest')
    assert resp.status == 200
    b = main_db_data['blocks'][-1]
    assert await resp.json() == {
        'difficulty': b['difficulty'],
        'extraData': b['extra_data'],
        'gasLimit': b['gas_limit'],
        'gasUsed': b['gas_used'],
        'hash': b['hash'],
        'logsBloom': b['logs_bloom'],
        'miner': b['miner'],
        'mixHash': b['mix_hash'],
        'nonce': b['nonce'],
        'number': b['number'],
        'parentHash': b['parent_hash'],
        'receiptsRoot': b['receipts_root'],
        'sha3Uncles': b['sha3_uncles'],
        'size': b['size'],
        'stateRoot': b['state_root'],
        'timestamp': b['timestamp'],
        'totalDifficulty': b['total_difficulty'],
        'transactions': [main_db_data['transactions'][-1]['hash']],
        'transactionsRoot': b['transactions_root'],
        'staticReward': b['static_reward'],
        'txFees': b['tx_fees'],
        'uncleInclusionReward': b['uncle_inclusion_reward'],
        'uncles': None
    }


async def test_get_account_404(cli):
    resp = await cli.get('/accounts/x')
    assert resp.status == 404


async def test_get_account(cli, main_db_data):
    resp = await cli.get('/accounts/' + main_db_data['accounts'][0]['address'])
    assert resp.status == 200
    a = main_db_data['accounts'][-1]
    assert await resp.json() == {'address': a['address'],
                                 'balance': a['balance'],
                                 'blockHash': a['block_hash'],
                                 'blockNumber': a['block_number'],
                                 'code': a['code'],
                                 'codeHash': a['code_hash'],
                                 'nonce': a['nonce']}


async def test_get_account_transactions(cli, main_db_data):
    resp = await cli.get('/accounts/' + main_db_data['accounts'][0]['address'] + '/transactions')
    assert resp.status == 200
    txs = main_db_data['transactions']
    res = await resp.json()
    assert len(res) == 4
    assert res[0]['hash'] == txs[0]['hash']
    assert res[1]['hash'] == txs[1]['hash']
    assert res[2]['hash'] == txs[2]['hash']
    assert res[3]['hash'] == txs[4]['hash']
    assert res[0] == {
        'blockHash': txs[0]['block_hash'],
        'blockNumber': txs[0]['block_number'],
        'from': txs[0]['from'],
        'gas': txs[0]['gas'],
        'gasPrice': txs[0]['gas_price'],
        'hash': txs[0]['hash'],
        'input': txs[0]['input'],
        'nonce': txs[0]['nonce'],
        'r': txs[0]['r'],
        's': txs[0]['s'],
        'to': txs[0]['to'],
        'transactionIndex': txs[0]['transaction_index'],
        'v': txs[0]['v'],
        'value': txs[0]['value'],
    }


async def test_get_block_transactions(cli, main_db_data):
    resp = await cli.get('/blocks/' + main_db_data['blocks'][1]['hash'] + '/transactions')
    assert resp.status == 200
    res = await resp.json()
    txs = main_db_data['transactions']
    assert len(res) == 2
    assert res[0] == {
        'blockHash': txs[0]['block_hash'],
        'blockNumber': txs[0]['block_number'],
        'from': txs[0]['from'],
        'gas': txs[0]['gas'],
        'gasPrice': txs[0]['gas_price'],
        'hash': txs[0]['hash'],
        'input': txs[0]['input'],
        'nonce': txs[0]['nonce'],
        'r': txs[0]['r'],
        's': txs[0]['s'],
        'to': txs[0]['to'],
        'transactionIndex': txs[0]['transaction_index'],
        'v': txs[0]['v'],
        'value': txs[0]['value'],
    }


@pytest.mark.usefixtures('uncles')
async def test_get_block_uncles(cli, main_db_data):
    resp = await cli.get('/blocks/' + main_db_data['blocks'][1]['hash'] + '/uncles')
    assert resp.status == 200
    assert await resp.json() == [
        {
            'difficulty': 17578564779,
            'blockNumber': 2,
            'extraData': '0x476574682f76312e302e302f6c696e75782f676f312e342e32',
            'gasLimit': 5000,
            'gasUsed': 0,
            'hash': '0x7852fb223883cd9af4cd9d448998c879a1f93a02954952666075df696c61a2cc',
            'logsBloom': '0x0',
            'miner': '0x0193d941b50d91be6567c7ee1c0fe7af498b4137',
            'mixHash': '0x94a09bb3ef9208bf434855efdb1089f80d07334d91930387a1f3150494e806cb',
            'nonce': '0x32de6ee381be0179',
            'number': 61,
            'parentHash': '0x3cd0324c7ba14ba7cf6e4b664dea0360681458d76bd25dfc0d2207ce4e9abed4',
            'receiptsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421',
            'sha3Uncles': '0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347',
            'size': None,
            'stateRoot': '0x1f4f1cf07f087191901752fe3da8ca195946366db6565f17afec5c04b3d75fd8',
            'timestamp': 1438270332,
            'totalDifficulty': None,
            'reward': 3750000000000000000,
            'transactionsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421'
        }
    ]


async def test_get_transaction(cli, main_db_data):
    tx = main_db_data['transactions'][0]
    resp = await cli.get('/transactions/' + tx['hash'])
    assert resp.status == 200
    assert await resp.json() == {
        'blockHash': tx['block_hash'],
        'blockNumber': tx['block_number'],
        'from': tx['from'],
        'gas': tx['gas'],
        'gasPrice': tx['gas_price'],
        'hash': tx['hash'],
        'input': tx['input'],
        'nonce': tx['nonce'],
        'r': tx['r'],
        's': tx['s'],
        'to': tx['to'],
        'transactionIndex': tx['transaction_index'],
        'v': tx['v'],
        'value': tx['value'],
    }


async def test_get_receipt(cli, main_db_data):
    r = main_db_data['receipts'][0]
    resp = await cli.get('/receipts/' + r['transaction_hash'])
    assert resp.status == 200
    res = await resp.json()
    assert res == {
        'blockHash': r['block_hash'],
        'blockNumber': r['block_number'],
        'contractAddress': r['contract_address'],
        'cumulativeGasUsed': r['cumulative_gas_used'],
        'from': r['from'],
        'gasUsed': r['gas_used'],
        'logs': [
            {'address': main_db_data['logs'][0]['address'],
             'blockHash': main_db_data['logs'][0]['block_hash'],
             'blockNumber': main_db_data['logs'][0]['block_number'],
             'data': main_db_data['logs'][0]['data'],
             'logIndex': main_db_data['logs'][0]['log_index'],
             'removed': main_db_data['logs'][0]['removed'],
             'topics': main_db_data['logs'][0]['topics'],
             'transactionHash': main_db_data['logs'][0]['transaction_hash'],
             'transactionIndex': main_db_data['logs'][0]['transaction_index'],
             }],
        'logsBloom': r['logs_bloom'],
        'root': r['root'],
        'status': r['status'],
        'to': r['to'],
        'transactionHash': r['transaction_hash'],
        'transactionIndex': r['transaction_index'],
    }


async def test_get_blocks_def(cli, main_db_data):
    b = main_db_data['blocks']
    resp = await cli.get('/blocks')
    assert resp.status == 200
    res = await resp.json()
    assert res[0]['hash'] == b[-1]['hash']
    assert res[1]['hash'] == b[-2]['hash']


async def test_get_blocks_ask(cli, main_db_data):
    resp = await cli.get('/blocks?order=asc')
    b = main_db_data['blocks']
    assert resp.status == 200
    res = await resp.json()
    assert res[0]['hash'] == b[0]['hash']
    assert res[1]['hash'] == b[1]['hash']


@pytest.mark.usefixtures('main_db_data')
async def test_get_blocks_limit_offset(cli):
    resp = await cli.get('/blocks?limit=1')
    assert resp.status == 200
    result = await resp.json()
    assert len(result) == 1
    assert result[0]['number'] == 5

    resp = await cli.get('/blocks?limit=1&offset=1')
    assert resp.status == 200
    result = await resp.json()
    assert len(result) == 1
    assert result[0]['number'] == 4


@pytest.mark.usefixtures('uncles')
async def test_get_uncles(cli, main_db_data):
    resp = await cli.get('/uncles')
    assert resp.status == 200
    assert await resp.json() == [
        {'blockNumber': main_db_data['blocks'][2]['number'],
         'difficulty': 18180751616,
         'extraData': '0x476574682f76312e302e302d30636463373634372f6c696e75782f676f312e34',
         'gasLimit': 5000,
         'gasUsed': 0,
         'hash': '0x6a5a801b12b94e1fb24e531b087719d699882a4f948564ba58706934bc5a19ff',
         'logsBloom': '0x0',
         'miner': '0x70137010922f2fc2964b3792907f79fbb75febe8',
         'mixHash': '0x48b762afc38197f6962c31851fd54ebbdff137bae3c64fff414eaa14ec243dbf',
         'nonce': '0x5283f7dfcd4a29ec',
         'number': 62,
         'parentHash': '0x5656b852baa80ce4db00c60998f5cf6e7a8d76f0339d3cf97955d933f731fecf',
         'receiptsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421',
         'sha3Uncles': '0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347',
         'size': None,
         'stateRoot': '0x901a42ee6ef09d68712df93609a8adbce98b314118d69a3dd07497615aa7b37b',
         'timestamp': 1438270505,
         'totalDifficulty': None,
         'reward': 3750000000000000000,
         'transactionsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421'},
        {'blockNumber': main_db_data['blocks'][1]['number'],
         'difficulty': 17578564779,
         'extraData': '0x476574682f76312e302e302f6c696e75782f676f312e342e32',
         'gasLimit': 5000,
         'gasUsed': 0,
         'hash': '0x7852fb223883cd9af4cd9d448998c879a1f93a02954952666075df696c61a2cc',
         'logsBloom': '0x0',
         'miner': '0x0193d941b50d91be6567c7ee1c0fe7af498b4137',
         'mixHash': '0x94a09bb3ef9208bf434855efdb1089f80d07334d91930387a1f3150494e806cb',
         'nonce': '0x32de6ee381be0179',
         'number': 61,
         'parentHash': '0x3cd0324c7ba14ba7cf6e4b664dea0360681458d76bd25dfc0d2207ce4e9abed4',
         'receiptsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421',
         'sha3Uncles': '0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347',
         'size': None,
         'stateRoot': '0x1f4f1cf07f087191901752fe3da8ca195946366db6565f17afec5c04b3d75fd8',
         'timestamp': 1438270332,
         'totalDifficulty': None,
         'reward': 3750000000000000000,
         'transactionsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421'}
    ]


@pytest.mark.usefixtures('uncles')
async def test_get_uncles_asc(cli):
    resp = await cli.get('/uncles?order=asc')
    assert resp.status == 200
    uncles = await resp.json()
    assert uncles[0]['number'] == 61
    assert uncles[1]['number'] == 62


@pytest.mark.usefixtures('uncles')
async def test_get_uncles_offset_limit(cli):
    resp = await cli.get('/uncles?offset=1&limit=1')
    assert resp.status == 200
    uncles = await resp.json()
    assert len(uncles) == 1
    assert uncles[0]['number'] == 61


@pytest.mark.usefixtures('uncles')
async def test_get_uncle_404(cli):
    resp = await cli.get('/uncles/111')
    assert resp.status == 404
    resp = await cli.get('/uncles/0x6a')
    assert resp.status == 404


@pytest.mark.usefixtures('uncles')
async def test_get_uncle_by_hash(cli, main_db_data):
    resp = await cli.get('/uncles/0x6a5a801b12b94e1fb24e531b087719d699882a4f948564ba58706934bc5a19ff')
    assert resp.status == 200
    assert await resp.json() == {
        'blockNumber': main_db_data['blocks'][2]['number'],
        'difficulty': 18180751616,
        'extraData': '0x476574682f76312e302e302d30636463373634372f6c696e75782f676f312e34',
        'gasLimit': 5000,
        'gasUsed': 0,
        'hash': '0x6a5a801b12b94e1fb24e531b087719d699882a4f948564ba58706934bc5a19ff',
        'logsBloom': '0x0',
        'miner': '0x70137010922f2fc2964b3792907f79fbb75febe8',
        'mixHash': '0x48b762afc38197f6962c31851fd54ebbdff137bae3c64fff414eaa14ec243dbf',
        'nonce': '0x5283f7dfcd4a29ec',
        'number': 62,
        'parentHash': '0x5656b852baa80ce4db00c60998f5cf6e7a8d76f0339d3cf97955d933f731fecf',
        'receiptsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421',
        'sha3Uncles': '0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347',
        'size': None,
        'stateRoot': '0x901a42ee6ef09d68712df93609a8adbce98b314118d69a3dd07497615aa7b37b',
        'timestamp': 1438270505,
        'totalDifficulty': None,
        'reward': 3750000000000000000,
        'transactionsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421'
    }


@pytest.mark.usefixtures('uncles')
async def test_get_uncle_by_number(cli, main_db_data):
    resp = await cli.get('/uncles/62')
    assert resp.status == 200
    assert await resp.json() == {
        'blockNumber': main_db_data['blocks'][2]['number'],
        'difficulty': 18180751616,
        'extraData': '0x476574682f76312e302e302d30636463373634372f6c696e75782f676f312e34',
        'gasLimit': 5000,
        'gasUsed': 0,
        'hash': '0x6a5a801b12b94e1fb24e531b087719d699882a4f948564ba58706934bc5a19ff',
        'logsBloom': '0x0',
        'miner': '0x70137010922f2fc2964b3792907f79fbb75febe8',
        'mixHash': '0x48b762afc38197f6962c31851fd54ebbdff137bae3c64fff414eaa14ec243dbf',
        'nonce': '0x5283f7dfcd4a29ec',
        'number': 62,
        'parentHash': '0x5656b852baa80ce4db00c60998f5cf6e7a8d76f0339d3cf97955d933f731fecf',
        'receiptsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421',
        'sha3Uncles': '0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347',
        'size': None,
        'stateRoot': '0x901a42ee6ef09d68712df93609a8adbce98b314118d69a3dd07497615aa7b37b',
        'timestamp': 1438270505,
        'totalDifficulty': None,
        'reward': 3750000000000000000,
        'transactionsRoot': '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421'
    }


class AsyncContextManagerMock(mock.Mock):
    async def __aenter__(self):
        return self.aenter

    async def __aexit__(self, *args):
        pass


async def test_verify_contract_ok(db, cli, main_db_data, here, fuck_token):
    contract_data = {
        'address': main_db_data['accounts'][2]['address'],
        'contract_name': 'FucksToken',
        'compiler': 'v0.4.18+commit.9cf6e910',
        'optimization_enabled': True,
        'constructor_args': None,
        'source_code': fuck_token.sources
    }

    with mock.patch('jsearch.api.handlers.aiohttp.request', new=AsyncContextManagerMock()) as m:
        m.return_value.aenter.json = CoroutineMock(side_effect=[
            {
                'bin': fuck_token.bin,
                'abi': fuck_token.abi_as_dict(),
            },
            {}
        ])

        resp = await cli.post('/verify_contract', json=contract_data)
    assert resp.status == 200
    assert await resp.json() == {'verification_passed': True}

    # assert m.has_call()
    m.assert_called_with(
        'POST',
        mock.ANY,
        json={
            'abi': fuck_token.abi_as_dict(),
            'address': contract_data['address'],
            'compiler': 'v0.4.18+commit.9cf6e910',
            'constructor_args': '',
            'contract_creation_code': mock.ANY,
            'contract_name': 'FucksToken',
            'is_erc20_token': True,
            'mhash': '4c3e25afac0b2393e51b49944bdfca9d02ac0c064fb7dccd895eaf7c59f55155',
            'optimization_enabled': True,
            'source_code': contract_data['source_code']
        }
    )


async def test_get_verified_contracts_list_ok(cli, main_db_data):
    contracts = main_db_data['contracts']

    resp = await cli.get('/verified_contracts')
    assert resp.status == 200
    res = await resp.json()
    assert len(res) == 1
    assert res == [{
        'name': contracts[0]['name'],
        'address': contracts[0]['address'],
        'byteCode': contracts[0]['byte_code'],
        'sourceCode': contracts[0]['source_code'],
        'abi': contracts[0]['abi'],
        'compilerVersion': contracts[0]['compiler_version'],
        'optimizationEnabled': contracts[0]['optimization_enabled'],
        'optimizationRuns': contracts[0]['optimization_runs'],
        'constructorArgs': contracts[0]['constructor_args'],
        'verified_at': contracts[0]['verified_at'],
    }]


async def test_get_verified_contract_ok(cli, main_db_data):
    contracts = main_db_data['contracts']

    resp = await cli.get('/verified_contracts/' + main_db_data['accounts'][2]['address'])
    assert resp.status == 200
    res = await resp.json()
    assert res == {
        'name': contracts[0]['name'],
        'address': contracts[0]['address'],
        'byteCode': contracts[0]['byte_code'],
        'sourceCode': contracts[0]['source_code'],
        'abi': contracts[0]['abi'],
        'compilerVersion': contracts[0]['compiler_version'],
        'optimizationEnabled': contracts[0]['optimization_enabled'],
        'optimizationRuns': contracts[0]['optimization_runs'],
        'constructorArgs': contracts[0]['constructor_args'],
        'verified_at': contracts[0]['verified_at'],
    }


async def test_get_tokens_list_ok(cli, main_db_data):
    contracts = main_db_data['contracts']
    resp = await cli.get('/tokens')
    assert resp.status == 200
    res = await resp.json()
    assert res == [{
        'name': contracts[0]['token_name'],
        'contractAddress': contracts[0]['address'],
        'decimals': contracts[0]['token_decimals'],
        'totalSupply': contracts[0]['token_total_supply'],
        'symbol': contracts[0]['token_symbol'],
    }]


async def test_get_token_ok(cli, main_db_data):
    contracts = main_db_data['contracts']

    resp = await cli.get('/tokens/' + main_db_data['accounts'][2]['address'])
    assert resp.status == 200
    res = await resp.json()
    assert res == {
        'name': contracts[0]['token_name'],
        'contractAddress': contracts[0]['address'],
        'decimals': contracts[0]['token_decimals'],
        'totalSupply': contracts[0]['token_total_supply'],
        'symbol': contracts[0]['token_symbol'],
    }


@pytest.mark.parametrize("account_index", [0, 1, 2])
async def test_get_account_token_transfers(cli, account_index, main_db_data, post_processing):
    # given
    dump = post_processing(main_db_data)
    account: AccountFromDumpWrapper = AccountFromDumpWrapper.from_dump(
        dump=dump,
        index=account_index
    )
    token_transfers: List[TokenTransferFromDumpWrapper] = TokenTransferFromDumpWrapper.from_dump(
        dump=dump,
        filters={
            'token_transfer_to': account.entity.address,
            'token_transfer_from': account.entity.address
        },
        strict=False,
        bulk=True
    )
    # when
    resp = await cli.get(f'/accounts/{account.entity.address}/token_transfers')

    # then
    assert resp.status == 200
    assert sort_token_transfers(await resp.json()) == sort_token_transfers(
        [transfer.as_dict() for transfer in token_transfers]
    )


async def test_get_token_transfers(cli, main_db_data, post_processing):
    # given
    dump = post_processing(main_db_data)
    receipt: ReceiptFromDumpWrapper = ReceiptFromDumpWrapper.from_dump(
        dump=dump,
        index=0
    )
    token_transfers: List[TokenTransferFromDumpWrapper] = TokenTransferFromDumpWrapper.from_dump(
        dump=dump,
        filters={
            'address': receipt.entity.contract_address,
            'is_token_transfer': True,
        },
        bulk=True
    )
    # when
    resp = await cli.get(f'/tokens/{receipt.entity.contract_address}/transfers')

    # then
    assert resp.status == 200
    assert sort_token_transfers(await resp.json()) == sort_token_transfers(
        [transfer.as_dict() for transfer in token_transfers]
    )


@pprint_returned_value
def sort_token_transfers(transfers):
    return sorted(
        transfers,
        key=lambda item: (item['blockHash'], item['transaction'], item['from'], item['to'], item['amount'])
    )
