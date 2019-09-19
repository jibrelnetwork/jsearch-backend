from urllib.parse import urlencode

import pytest
from aiohttp.test_utils import TestClient
from typing import Callable, Awaitable, NamedTuple

from jsearch.api.storage import Storage
from jsearch.api.structs import BlockchainTip, BlockInfo
from jsearch.tests.plugins.databases.factories.accounts import AccountStateFactory, AccountFactory
from jsearch.tests.plugins.databases.factories.blocks import BlockFactory
from jsearch.tests.plugins.databases.factories.internal_transactions import InternalTransactionFactory
from jsearch.tests.plugins.databases.factories.logs import LogFactory
from jsearch.tests.plugins.databases.factories.reorgs import ReorgFactory
from jsearch.tests.plugins.databases.factories.token_holder import TokenHolderFactory
from jsearch.tests.plugins.databases.factories.token_transfers import TokenTransferFactory
from jsearch.tests.plugins.databases.factories.transactions import TransactionFactory
from jsearch.tests.plugins.databases.factories.uncles import UncleFactory
from jsearch.tests.plugins.databases.factories.wallet_events import WalletEventsFactory

API_STATUS_SUCCESS = {
    "success": True,
    "errors": []
}

API_META_TIP_FORKED = {
    "blockchainTipStatus": {
        "blockHash": "0xforked",
        "blockNumber": 11,
        "isOrphaned": True,
        "lastUnchangedBlock": 10,
    },
    "currentBlockchainTip": {
        "blockHash": "0x03225db5f45479904b9e0f5c8311c5267a43beaf8e92bc323a0a5315b38a9d5e",
        "blockNumber": 100,
    }
}

API_META_TIP_CANONICAL = {
    "blockchainTipStatus": {
        "blockHash": "0xcanonical",
        "blockNumber": 11,
        "isOrphaned": False,
        "lastUnchangedBlock": None,
    },
    "currentBlockchainTip": {
        "blockHash": "0x03225db5f45479904b9e0f5c8311c5267a43beaf8e92bc323a0a5315b38a9d5e",
        "blockNumber": 100,
    }
}

TipGetter = Callable[[bool], Awaitable[BlockchainTip]]

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.usefixtures('disable_metrics_setup'),
]


class BlockchainTipCase(NamedTuple):
    is_tip_forked: bool
    is_data_recent: bool
    has_empty_data_response: bool

    @property
    def api_meta(self):
        return API_META_TIP_FORKED if self.is_tip_forked else API_META_TIP_CANONICAL


cases = [
    BlockchainTipCase(
        is_tip_forked=True,
        is_data_recent=True,
        has_empty_data_response=True,
    ),
    BlockchainTipCase(
        is_tip_forked=True,
        is_data_recent=False,
        has_empty_data_response=False,
    ),
    BlockchainTipCase(
        is_tip_forked=False,
        is_data_recent=True,
        has_empty_data_response=False,
    ),
    BlockchainTipCase(
        is_tip_forked=False,
        is_data_recent=False,
        has_empty_data_response=False,
    ),
]


@pytest.fixture()
def _get_tip(
        storage: Storage,
        block_factory: BlockFactory,
        chain_events_factory,
        reorg_factory: ReorgFactory,
) -> Callable[[bool], Awaitable[BlockchainTip]]:
    async def inner(is_forked: bool) -> BlockchainTip:
        common_block = block_factory.create(number=10)

        canonical_block = block_factory.create(parent_hash=common_block.hash, hash='0xcanonical', number=11)
        forked_block = block_factory.create(parent_hash=common_block.hash, hash='0xforked', number=11, is_forked=True)

        # WTF: Making last block for consistent `currentBlockchainTip`.
        block_factory.create(
            hash='0x03225db5f45479904b9e0f5c8311c5267a43beaf8e92bc323a0a5315b38a9d5e',
            number=100,
        )

        chain_splits = chain_events_factory.create(
            block_hash=common_block.hash,
            block_number=common_block.number,
        )
        reorg_factory.create(
            block_hash=forked_block.hash,
            block_number=forked_block.number,
            split_id=chain_splits.id,
        )

        tip_block = forked_block if is_forked else canonical_block
        tip_block_info = BlockInfo(number=tip_block.number, hash=tip_block.hash, timestamp=0)
        tip = await storage.get_blockchain_tip(tip_block_info)

        return tip

    return inner


@pytest.mark.parametrize('case', cases, ids=[repr(c) for c in cases])
async def test_get_accounts_balances_with_tip(
        cli: TestClient,
        account_state_factory: AccountStateFactory,
        case: BlockchainTipCase,
        _get_tip: TipGetter,
) -> None:
    tip = await _get_tip(case.is_tip_forked)

    target_block_number = tip.tip_number + 5 if case.is_data_recent else tip.tip_number - 5
    account_state = account_state_factory.create(
        address='0xcd424c53f5dc7d22cdff536309c24ad87a97e6af',
        block_number=target_block_number,
        balance=256391824440000,
    )

    response = await cli.get(f'/v1/accounts/balances?addresses={account_state.address}&blockchain_tip={tip.tip_hash}')
    response_json = await response.json()
    response_json.pop('paging', None)

    data = [] if case.has_empty_data_response else [
        {
            "balance": str(256391824440000),
            "address": "0xcd424c53f5dc7d22cdff536309c24ad87a97e6af"
        },
    ]

    assert response_json == {
        "status": API_STATUS_SUCCESS,
        "data": data,
        "meta": case.api_meta,
    }


@pytest.mark.parametrize('case', cases, ids=[repr(c) for c in cases])
async def test_get_account_with_tip(
        cli: TestClient,
        account_factory: AccountFactory,
        account_state_factory: AccountStateFactory,
        case: BlockchainTipCase,
        _get_tip: TipGetter,
) -> None:
    tip = await _get_tip(case.is_tip_forked)

    target_block_number = tip.tip_number + 5 if case.is_data_recent else tip.tip_number - 5
    account = account_factory.create(
        address='0xcd424c53f5dc7d22cdff536309c24ad87a97e6af',
        code='',
        code_hash='c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470',
    )
    account_state_factory.create(
        block_number=target_block_number,
        block_hash='0x0e851d527ca5b1a8356a29d198c920f20da9af51edc084acaa0de481324d8f5d',
        address='0xcd424c53f5dc7d22cdff536309c24ad87a97e6af',
        nonce=976,
        balance=1029436321514224,
    )

    response = await cli.get(f'/v1/accounts/{account.address}?tag=latest&blockchain_tip={tip.tip_hash}')
    response_json = await response.json()
    response_json.pop('paging', None)

    data = dict() if case.has_empty_data_response else {
        "blockNumber": target_block_number,
        "blockHash": "0x0e851d527ca5b1a8356a29d198c920f20da9af51edc084acaa0de481324d8f5d",
        "address": "0xcd424c53f5dc7d22cdff536309c24ad87a97e6af",
        "nonce": 976,
        "code": "0x",
        "codeHash": "0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470",
        "balance": str(1029436321514224),
    }

    assert response_json == {
        "status": API_STATUS_SUCCESS,
        "data": data,
        "meta": case.api_meta,
    }


@pytest.mark.parametrize('case', cases, ids=[repr(c) for c in cases])
async def test_get_account_transactions_with_tip(
        cli: TestClient,
        transaction_factory: TransactionFactory,
        case: BlockchainTipCase,
        _get_tip: TipGetter,
) -> None:
    tip = await _get_tip(case.is_tip_forked)

    target_block_number = tip.tip_number + 5 if case.is_data_recent else tip.tip_number - 5
    transaction = transaction_factory.create(
        from_='0x3a844524342f0',
        to='0xcd424c53f5dc7d22cdff536309c24ad87a97e6af',
        address='0xcd424c53f5dc7d22cdff536309c24ad87a97e6af',
        hash='0xf096ab24c5bd8abd9298cd627f5eef1ee948776d8d11127d8c47da2f0897f2c5',
        timestamp='1453686776',
        transaction_index='84',
        block_number=target_block_number,
        block_hash='0x2f571cb815c2d94c8e48bf697799e545c368029e8b096a730ef5e650874fbbad',
        gas='25000',
        gas_price='50000000000',
        input='0x',
        nonce='543',
        r='0x23e819fa3f631c042d20b70f28f8f08ef1a2733061b92c59b43ea0997b6cf834',
        s='0x1ad76eadafc639103f6ba7bc0b9f839757086669b973e601ab69efda745948e3',
        v='0x1c',
        value='2808270086200000000',
    )

    query_params = f'block_number={target_block_number}&limit=1&blockchain_tip={tip.tip_hash}'
    response = await cli.get(f'/v1/accounts/{transaction.address}/transactions?{query_params}')
    response_json = await response.json()
    response_json.pop('paging', None)

    data = [] if case.has_empty_data_response else [
        {
            "blockHash": "0x2f571cb815c2d94c8e48bf697799e545c368029e8b096a730ef5e650874fbbad",
            "blockNumber": target_block_number,
            "status": 1,
            "from": "0x3a844524342f0",
            "gas": "25000",
            "gasPrice": "50000000000",
            "hash": "0xf096ab24c5bd8abd9298cd627f5eef1ee948776d8d11127d8c47da2f0897f2c5",
            "input": "0x",
            "nonce": "543",
            "r": "0x23e819fa3f631c042d20b70f28f8f08ef1a2733061b92c59b43ea0997b6cf834",
            "s": "0x1ad76eadafc639103f6ba7bc0b9f839757086669b973e601ab69efda745948e3",
            "to": "0xcd424c53f5dc7d22cdff536309c24ad87a97e6af",
            "transactionIndex": 84,
            "v": "0x1c",
            "value": "2808270086200000000",
            "timestamp": 1453686776,
        }
    ]

    assert response_json == {
        "status": API_STATUS_SUCCESS,
        "data": data,
        "meta": case.api_meta,
    }


@pytest.mark.parametrize('case', cases, ids=[repr(c) for c in cases])
async def test_get_account_internal_transactions_with_tip(
        cli: TestClient,
        internal_transaction_factory: InternalTransactionFactory,
        case: BlockchainTipCase,
        _get_tip: TipGetter,
) -> None:
    tip = await _get_tip(case.is_tip_forked)

    target_block_number = tip.tip_number + 5 if case.is_data_recent else tip.tip_number - 5
    internal_tx = internal_transaction_factory.create(
        block_number=target_block_number,
        block_hash='0x2f571cb815c2d94c8e48bf697799e545c368029e8b096a730ef5e650874fbbad',
        timestamp=1550000000,
        parent_tx_hash='0xf096ab24c5bd8abd9298cd627f5eef1ee948776d8d11127d8c47da2f0897f2c5',
        parent_tx_index=1,
        op='suicide',
        call_depth='2',
        tx_origin='0xab515c53f5dc7d22cdff536309c24ad87a9fe6af',
        from_='0xab515c53f5dc7d22cdff536309c24ad87a9fe6af',
        to='0xcd424c53f5dc7d22cdff536309c24ad87a97e6af',
        value='41000000000',
        gas_limit='2300',
        payload='0x',
        status='success',
        transaction_index='84',
    )
    account_address = getattr(internal_tx, 'from')

    query_params = f'block_number={target_block_number}&limit=1&blockchain_tip={tip.tip_hash}'
    response = await cli.get(f'/v1/accounts/{account_address}/internal_transactions?{query_params}')
    response_json = await response.json()
    response_json.pop('paging', None)

    data = [] if case.has_empty_data_response else [
        {
            "blockNumber": target_block_number,
            "blockHash": "0x2f571cb815c2d94c8e48bf697799e545c368029e8b096a730ef5e650874fbbad",
            "timestamp": 1550000000,
            "parentTxHash": "0xf096ab24c5bd8abd9298cd627f5eef1ee948776d8d11127d8c47da2f0897f2c5",
            "parentTxIndex": 1,
            "op": "suicide",
            "callDepth": 2,
            "from": "0xab515c53f5dc7d22cdff536309c24ad87a9fe6af",
            "to": "0xcd424c53f5dc7d22cdff536309c24ad87a97e6af",
            "value": "41000000000",
            "gasLimit": "2300",
            "input": "0x",
            "status": "success",
            "transactionIndex": 84
        }
    ]

    assert response_json == {
        "status": API_STATUS_SUCCESS,
        "data": data,
        "meta": case.api_meta,
    }


@pytest.mark.parametrize('case', cases, ids=[repr(c) for c in cases])
async def test_get_account_mined_blocks_with_tip(
        cli: TestClient,
        block_factory: BlockFactory,
        case: BlockchainTipCase,
        _get_tip: TipGetter,
) -> None:
    tip = await _get_tip(case.is_tip_forked)

    target_block_number = tip.tip_number + 5 if case.is_data_recent else tip.tip_number - 5
    block = block_factory.create(
        hash='0x88a6bc42f4f65a0daab3a810444c2202d301db04d05203a86342b35333ac1413',
        parent_hash='0x9e4f201db6e56a43980881cd09855b99b2f2aeefc84ffb2ad0ccf3f42de6fba2',
        number=target_block_number,
        difficulty='10694243015446',
        gas_used='0',
        miner='0xf8b483dba2c3b7176a3da549ad41a48bb3121069',
        tx_fees='411095732236680000',
        timestamp='1453686776',
        gas_limit='3141592',
        static_reward='411095732236680000',
        extra_data='0xd983010302844765746887676f312e342e328777696e646f7773',
        logs_bloom='0x00000000000000000000000000000000000000000000000000000000001',
        mix_hash='0x02a775f306082912b617e858fef268597a277de056dbe924ee6aabfa35a33c44',
        nonce='496358969209982823',
        sha3_uncles='0x2843dd2134eb02067b585e76ce6a7fc89d22d3eae1d38827b1eb15a3b5153347',
        state_root='0xc27aca6363fdceaed835753083b4db0bc37fab441e1414b9f051047d37dd025f',
        receipts_root='0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421',
        total_difficulty='10694243015446',
        transactions_root='0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421',
        uncle_inclusion_reward='0',
    )
    query_params = f'block_number={target_block_number}&limit=1&blockchain_tip={tip.tip_hash}'
    response = await cli.get(f'/v1/accounts/{block.miner}/mined_blocks?{query_params}')
    response_json = await response.json()
    response_json.pop('paging', None)

    data = [] if case.has_empty_data_response else [
        {
            "difficulty": "10694243015446",
            "extraData": "0xd983010302844765746887676f312e342e328777696e646f7773",
            "gasLimit": "3141592",
            "gasUsed": "0",
            "hash": "0x88a6bc42f4f65a0daab3a810444c2202d301db04d05203a86342b35333ac1413",
            "logsBloom": "0x00000000000000000000000000000000000000000000000000000000001",
            "miner": "0xf8b483dba2c3b7176a3da549ad41a48bb3121069",
            "mixHash": "0x02a775f306082912b617e858fef268597a277de056dbe924ee6aabfa35a33c44",
            "nonce": "496358969209982823",
            "number": target_block_number,
            "parentHash": "0x9e4f201db6e56a43980881cd09855b99b2f2aeefc84ffb2ad0ccf3f42de6fba2",
            "receiptsRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            "sha3Uncles": "0x2843dd2134eb02067b585e76ce6a7fc89d22d3eae1d38827b1eb15a3b5153347",
            "stateRoot": "0xc27aca6363fdceaed835753083b4db0bc37fab441e1414b9f051047d37dd025f",
            "timestamp": 1453686776,
            "transactions": None,
            "transactionsRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            "uncles": None,
            "staticReward": str(411095732236680000),
            "uncleInclusionReward": str(0),
            "txFees": str(411095732236680000),
        }
    ]

    assert response_json == {
        "status": API_STATUS_SUCCESS,
        "data": data,
        "meta": case.api_meta,
    }


@pytest.mark.parametrize('case', cases, ids=[repr(c) for c in cases])
async def test_get_account_mined_uncles_with_tip(
        cli: TestClient,
        uncle_factory: UncleFactory,
        case: BlockchainTipCase,
        _get_tip: TipGetter,
) -> None:
    tip = await _get_tip(case.is_tip_forked)

    target_block_number = tip.tip_number + 5 if case.is_data_recent else tip.tip_number - 5
    uncle = uncle_factory.create(
        hash='0x88a6bc42f4f65a0daab3a810444c2202d301db04d05203a86342b35333ac1413',
        parent_hash='0x9e4f201db6e56a43980881cd09855b99b2f2aeefc84ffb2ad0ccf3f42de6fba2',
        number=tip.tip_number,
        block_number=target_block_number,
        difficulty='10694243015446',
        gas_used='0',
        miner='0xf8b483dba2c3b7176a3da549ad41a48bb3121069',
        reward='411095732236680000',
        timestamp='1453686776',
        gas_limit='3141592',
        extra_data='0xd983010302844765746887676f312e342e328777696e646f7773',
        logs_bloom='0x00000000000000000000000000000000000000000000000000000000001',
        mix_hash='0x02a775f306082912b617e858fef268597a277de056dbe924ee6aabfa35a33c44',
        nonce='496358969209982823',
        sha3_uncles='0x2843dd2134eb02067b585e76ce6a7fc89d22d3eae1d38827b1eb15a3b5153347',
        state_root='0xc27aca6363fdceaed835753083b4db0bc37fab441e1414b9f051047d37dd025f',
        receipts_root='0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421',
        total_difficulty='10694243015446',
        transactions_root='0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421',
    )
    query_params = f'uncle_number={tip.tip_number}&limit=1&blockchain_tip={tip.tip_hash}'
    response = await cli.get(f'/v1/accounts/{uncle.miner}/mined_uncles?{query_params}')
    response_json = await response.json()
    response_json.pop('paging', None)

    data = [] if case.has_empty_data_response else [
        {
            "difficulty": "10694243015446",
            "extraData": "0xd983010302844765746887676f312e342e328777696e646f7773",
            "gasLimit": "3141592",
            "gasUsed": "0",
            "hash": "0x88a6bc42f4f65a0daab3a810444c2202d301db04d05203a86342b35333ac1413",
            "logsBloom": "0x00000000000000000000000000000000000000000000000000000000001",
            "miner": "0xf8b483dba2c3b7176a3da549ad41a48bb3121069",
            "mixHash": "0x02a775f306082912b617e858fef268597a277de056dbe924ee6aabfa35a33c44",
            "nonce": "496358969209982823",
            "number": tip.tip_number,
            "parentHash": "0x9e4f201db6e56a43980881cd09855b99b2f2aeefc84ffb2ad0ccf3f42de6fba2",
            "receiptsRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            "sha3Uncles": "0x2843dd2134eb02067b585e76ce6a7fc89d22d3eae1d38827b1eb15a3b5153347",
            "stateRoot": "0xc27aca6363fdceaed835753083b4db0bc37fab441e1414b9f051047d37dd025f",
            "timestamp": 1453686776,
            "transactionsRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            "blockNumber": target_block_number,
            "reward": str(411095732236680000),
        }
    ]

    assert response_json == {
        "status": API_STATUS_SUCCESS,
        "data": data,
        "meta": case.api_meta,
    }


@pytest.mark.parametrize('case', cases, ids=[repr(c) for c in cases])
async def test_get_account_token_transfers_with_tip(
        cli: TestClient,
        transfer_factory: TokenTransferFactory,
        block_factory: BlockFactory,
        transaction_factory: TransactionFactory,
        log_factory: LogFactory,
        case: BlockchainTipCase,
        _get_tip: TipGetter,
) -> None:
    tip = await _get_tip(case.is_tip_forked)

    target_block_number = tip.tip_number + 5 if case.is_data_recent else tip.tip_number - 5
    block = block_factory.create(number=target_block_number)
    tx = transaction_factory.create_for_block(block)[0]
    log = log_factory.create_for_tx(tx)
    transfer = transfer_factory.create_for_log(block, tx, log)[0]

    query_params = urlencode({
        'block_number': target_block_number,
        'blockchain_tip': tip.tip_hash,
        'limit': 1,
    })

    response = await cli.get(f'/v1/accounts/{transfer.address}/token_transfers?{query_params}')
    response_json = await response.json()
    response_json.pop('paging', None)

    data = [] if case.has_empty_data_response else [
        {
            'amount': f'{int(transfer.token_value)}',
            'blockNumber': transfer.block_number,
            'contractAddress': transfer.token_address,
            'decimals': transfer.token_decimals,
            'from': transfer.from_address,
            'timestamp': transfer.timestamp,
            'to': transfer.to_address,
            'transactionHash': transfer.transaction_hash,
            'transactionIndex': transfer.transaction_index,
            'logIndex': transfer.log_index,
        }
    ]

    assert response_json == {
        "status": API_STATUS_SUCCESS,
        "data": data,
        "meta": case.api_meta,
    }


@pytest.mark.parametrize('case', cases, ids=[repr(c) for c in cases])
async def test_get_account_token_balance_with_tip(
        cli: TestClient,
        token_holder_factory: TokenHolderFactory,
        case: BlockchainTipCase,
        _get_tip: TipGetter,
) -> None:
    tip = await _get_tip(case.is_tip_forked)

    target_block_number = tip.tip_number + 5 if case.is_data_recent else tip.tip_number - 5
    token_holder = token_holder_factory.create(
        block_number=target_block_number,
        account_address='0xfdbacd53b94c4e76742f66a9f235a5d1e5218bb0',
        token_address='0xa5fd1a791c4dfcaacc963d4f73c6ae5824149ea7',
        balance='1000000',
        decimals='18',
    )

    address = token_holder.account_address
    token_address = token_holder.token_address

    response = await cli.get(f'/v1/accounts/{address}/token_balance/{token_address}?blockchain_tip={tip.tip_hash}')
    response_json = await response.json()
    response_json.pop('paging', None)

    data = dict() if case.has_empty_data_response else {
        "accountAddress": "0xfdbacd53b94c4e76742f66a9f235a5d1e5218bb0",
        "contractAddress": "0xa5fd1a791c4dfcaacc963d4f73c6ae5824149ea7",
        "balance": '1000000',
        "decimals": 18,
    }

    assert response_json == {
        "status": API_STATUS_SUCCESS,
        "data": data,
        "meta": case.api_meta,
    }


@pytest.mark.parametrize('case', cases, ids=[repr(c) for c in cases])
async def test_get_account_logs_with_tip(
        cli: TestClient,
        log_factory: LogFactory,
        case: BlockchainTipCase,
        _get_tip: TipGetter,
) -> None:
    tip = await _get_tip(case.is_tip_forked)

    target_block_number = tip.tip_number + 5 if case.is_data_recent else tip.tip_number - 5
    log = log_factory.create(
        block_number=target_block_number,
        block_hash='0x4c285ba67d33a3cd670f5c4decfb10a41b929e7c4139766abfd60a24ee1fa148',
        timestamp=1561100257,
        log_index='0',
        address='0x47071214d1ef76eeb26e9ac3ec6cc965ab8eb75b',
        data='0x00000000000000000000000013f26856cbacaaba9c4488a31c72e605fae029fc',
        removed=False,
        topics=[
            "0x16cdf1707799c6655baac6e210f52b94b7cec08adcaf9ede7dfe8649da926146"
        ],
        transaction_hash='0xcb63b762d9522bbd712b0d8df2208c8a8dbdaeef5d7fdca3cc2dad0f34646790',
        transaction_index='2',
    )

    query_params = f'block_number={target_block_number}&limit=1&blockchain_tip={tip.tip_hash}'
    response = await cli.get(f'/v1/accounts/{log.address}/logs?{query_params}')
    response_json = await response.json()
    response_json.pop('paging', None)

    data = [] if case.has_empty_data_response else [
        {
            "address": "0x47071214d1ef76eeb26e9ac3ec6cc965ab8eb75b",
            "blockHash": "0x4c285ba67d33a3cd670f5c4decfb10a41b929e7c4139766abfd60a24ee1fa148",
            "blockNumber": target_block_number,
            "timestamp": 1561100257,
            "data": "0x00000000000000000000000013f26856cbacaaba9c4488a31c72e605fae029fc",
            "logIndex": 0,
            "removed": False,
            "topics": [
                "0x16cdf1707799c6655baac6e210f52b94b7cec08adcaf9ede7dfe8649da926146"
            ],
            "transactionHash": "0xcb63b762d9522bbd712b0d8df2208c8a8dbdaeef5d7fdca3cc2dad0f34646790",
            "transactionIndex": 2
        }
    ]

    assert response_json == {
        "status": API_STATUS_SUCCESS,
        "data": data,
        "meta": case.api_meta,
    }


@pytest.mark.parametrize('case', cases, ids=[repr(c) for c in cases])
async def test_get_blocks_with_tip(
        cli: TestClient,
        block_factory: BlockFactory,
        case: BlockchainTipCase,
        _get_tip: TipGetter,
) -> None:
    tip = await _get_tip(case.is_tip_forked)

    target_block_number = tip.tip_number + 5 if case.is_data_recent else tip.tip_number - 5
    block_factory.create(
        hash='0x88a6bc42f4f65a0daab3a810444c2202d301db04d05203a86342b35333ac1413',
        parent_hash='0x9e4f201db6e56a43980881cd09855b99b2f2aeefc84ffb2ad0ccf3f42de6fba2',
        number=target_block_number,
        difficulty='10694243015446',
        gas_used='0',
        miner='0xf8b483dba2c3b7176a3da549ad41a48bb3121069',
        tx_fees='411095732236680000',
        timestamp='1453686776',
        gas_limit='3141592',
        static_reward='411095732236680000',
        extra_data='0xd983010302844765746887676f312e342e328777696e646f7773',
        logs_bloom='0x00000000000000000000000000000000000000000000000000000000001',
        mix_hash='0x02a775f306082912b617e858fef268597a277de056dbe924ee6aabfa35a33c44',
        nonce='496358969209982823',
        sha3_uncles='0x2843dd2134eb02067b585e76ce6a7fc89d22d3eae1d38827b1eb15a3b5153347',
        state_root='0xc27aca6363fdceaed835753083b4db0bc37fab441e1414b9f051047d37dd025f',
        receipts_root='0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421',
        total_difficulty='10694243015446',
        transactions_root='0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421',
        uncle_inclusion_reward='0',
    )

    # WTF: Misc blocks are created in `_get_tip`, so select only target block to
    # validate.
    query_params = f'block_number={target_block_number}&limit=1&blockchain_tip={tip.tip_hash}'
    response = await cli.get(f'/v1/blocks?{query_params}')
    response_json = await response.json()
    response_json.pop('paging', None)

    data = [] if case.has_empty_data_response else [
        {
            "difficulty": "10694243015446",
            "extraData": "0xd983010302844765746887676f312e342e328777696e646f7773",
            "gasLimit": "3141592",
            "gasUsed": "0",
            "hash": "0x88a6bc42f4f65a0daab3a810444c2202d301db04d05203a86342b35333ac1413",
            "logsBloom": "0x00000000000000000000000000000000000000000000000000000000001",
            "miner": "0xf8b483dba2c3b7176a3da549ad41a48bb3121069",
            "mixHash": "0x02a775f306082912b617e858fef268597a277de056dbe924ee6aabfa35a33c44",
            "nonce": "496358969209982823",
            "number": target_block_number,
            "parentHash": "0x9e4f201db6e56a43980881cd09855b99b2f2aeefc84ffb2ad0ccf3f42de6fba2",
            "receiptsRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            "sha3Uncles": "0x2843dd2134eb02067b585e76ce6a7fc89d22d3eae1d38827b1eb15a3b5153347",
            "stateRoot": "0xc27aca6363fdceaed835753083b4db0bc37fab441e1414b9f051047d37dd025f",
            "timestamp": 1453686776,
            "transactions": None,
            "transactionsRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            "uncles": None,
            "staticReward": str(411095732236680000),
            "uncleInclusionReward": str(0),
            "txFees": str(411095732236680000),
        }
    ]

    assert response_json == {
        "status": API_STATUS_SUCCESS,
        "data": data,
        "meta": case.api_meta,
    }


@pytest.mark.parametrize('case', cases, ids=[repr(c) for c in cases])
async def test_get_uncles_with_tip(
        cli: TestClient,
        uncle_factory: UncleFactory,
        case: BlockchainTipCase,
        _get_tip: TipGetter,
) -> None:
    tip = await _get_tip(case.is_tip_forked)

    target_block_number = tip.tip_number + 5 if case.is_data_recent else tip.tip_number - 5
    uncle_number = target_block_number - 1
    uncle_factory.create(
        hash='0x88a6bc42f4f65a0daab3a810444c2202d301db04d05203a86342b35333ac1413',
        parent_hash='0x9e4f201db6e56a43980881cd09855b99b2f2aeefc84ffb2ad0ccf3f42de6fba2',
        number=uncle_number,
        block_number=target_block_number,
        difficulty='10694243015446',
        gas_used='0',
        miner='0xf8b483dba2c3b7176a3da549ad41a48bb3121069',
        reward='411095732236680000',
        timestamp='1453686776',
        gas_limit='3141592',
        extra_data='0xd983010302844765746887676f312e342e328777696e646f7773',
        logs_bloom='0x00000000000000000000000000000000000000000000000000000000001',
        mix_hash='0x02a775f306082912b617e858fef268597a277de056dbe924ee6aabfa35a33c44',
        nonce='496358969209982823',
        sha3_uncles='0x2843dd2134eb02067b585e76ce6a7fc89d22d3eae1d38827b1eb15a3b5153347',
        state_root='0xc27aca6363fdceaed835753083b4db0bc37fab441e1414b9f051047d37dd025f',
        receipts_root='0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421',
        total_difficulty='10694243015446',
        transactions_root='0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421',
    )

    query_params = f'uncle_number={uncle_number}&limit=1&blockchain_tip={tip.tip_hash}'
    response = await cli.get(f'/v1/uncles?{query_params}')
    response_json = await response.json()
    response_json.pop('paging', None)

    data = [] if case.has_empty_data_response else [
        {
            "difficulty": "10694243015446",
            "extraData": "0xd983010302844765746887676f312e342e328777696e646f7773",
            "gasLimit": "3141592",
            "gasUsed": "0",
            "hash": "0x88a6bc42f4f65a0daab3a810444c2202d301db04d05203a86342b35333ac1413",
            "logsBloom": "0x00000000000000000000000000000000000000000000000000000000001",
            "miner": "0xf8b483dba2c3b7176a3da549ad41a48bb3121069",
            "mixHash": "0x02a775f306082912b617e858fef268597a277de056dbe924ee6aabfa35a33c44",
            "nonce": "496358969209982823",
            "number": uncle_number,
            "parentHash": "0x9e4f201db6e56a43980881cd09855b99b2f2aeefc84ffb2ad0ccf3f42de6fba2",
            "receiptsRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            "sha3Uncles": "0x2843dd2134eb02067b585e76ce6a7fc89d22d3eae1d38827b1eb15a3b5153347",
            "stateRoot": "0xc27aca6363fdceaed835753083b4db0bc37fab441e1414b9f051047d37dd025f",
            "timestamp": 1453686776,
            "transactionsRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
            "blockNumber": target_block_number,
            "reward": str(411095732236680000),
        }
    ]

    assert response_json == {
        "status": API_STATUS_SUCCESS,
        "data": data,
        "meta": case.api_meta,
    }


@pytest.mark.parametrize('case', cases, ids=[repr(c) for c in cases])
async def test_get_token_transfers_with_tip(
        cli: TestClient,
        block_factory: BlockFactory,
        transaction_factory: TransactionFactory,
        log_factory: LogFactory,
        transfer_factory: TokenTransferFactory,
        case: BlockchainTipCase,
        _get_tip: TipGetter,
) -> None:
    tip = await _get_tip(case.is_tip_forked)

    target_block_number = tip.tip_number + 5 if case.is_data_recent else tip.tip_number - 5

    block = block_factory.create(number=target_block_number)
    tx = transaction_factory.create_for_block(block)[0]
    log = log_factory.create_for_tx(tx)
    transfer = transfer_factory.create_for_log(block, tx, log)[0]

    query_params = urlencode({
        'block_number': target_block_number,
        'blockchain_tip': tip.tip_hash,
        'limit': 1,
    })
    response = await cli.get(f'/v1/tokens/{transfer.token_address}/transfers?{query_params}')
    response_json = await response.json()
    response_json.pop('paging', None)

    data = [] if case.has_empty_data_response else [
        {
            'amount': f'{int(transfer.token_value)}',
            'blockNumber': transfer.block_number,
            'contractAddress': transfer.token_address,
            'decimals': transfer.token_decimals,
            'from': transfer.from_address,
            'timestamp': transfer.timestamp,
            'to': transfer.to_address,
            'transactionHash': transfer.transaction_hash,
            'transactionIndex': transfer.transaction_index,
            'logIndex': transfer.log_index,
        }
    ]

    assert response_json == {
        "status": API_STATUS_SUCCESS,
        "data": data,
        "meta": case.api_meta,
    }


@pytest.mark.parametrize('case', cases, ids=[repr(c) for c in cases])
async def test_get_token_holders_with_tip(
        cli: TestClient,
        token_holder_factory: TokenHolderFactory,
        case: BlockchainTipCase,
        _get_tip: TipGetter,
) -> None:
    tip = await _get_tip(case.is_tip_forked)

    target_block_number = tip.tip_number + 5 if case.is_data_recent else tip.tip_number - 5
    token_holder = token_holder_factory.create(
        block_number=target_block_number,
        account_address="0xfdbacd53b94c4e76742f66a9f235a5d1e5218bb0",
        token_address="0xa5fd1a791c4dfcaacc963d4f73c6ae5824149ea7",
        balance="1000000",
        decimals="18",
    )

    query_params = f'block_number={target_block_number}&limit=1&blockchain_tip={tip.tip_hash}'
    response = await cli.get(f'/v1/tokens/{token_holder.token_address}/holders?{query_params}')
    response_json = await response.json()
    response_json.pop('paging', None)

    data = [] if case.has_empty_data_response else [
        {
            "accountAddress": "0xfdbacd53b94c4e76742f66a9f235a5d1e5218bb0",
            "contractAddress": "0xa5fd1a791c4dfcaacc963d4f73c6ae5824149ea7",
            "balance": 1000000,
            "decimals": 18,
            "id": 0
        }
    ]

    assert response_json == {
        "status": API_STATUS_SUCCESS,
        "data": data,
        "meta": case.api_meta,
    }


@pytest.mark.parametrize('case', cases, ids=[repr(c) for c in cases])
async def test_get_wallet_events_with_tip(
        cli: TestClient,
        block_factory: BlockFactory,
        transaction_factory: TransactionFactory,
        wallet_events_factory: WalletEventsFactory,
        case: BlockchainTipCase,
        _get_tip: TipGetter,
) -> None:
    tip = await _get_tip(case.is_tip_forked)

    target_block_number = tip.tip_number + 5 if case.is_data_recent else tip.tip_number - 5

    block = block_factory.create(number=target_block_number)
    tx, _ = transaction_factory.create_for_block(block=block)
    event = wallet_events_factory.create_token_transfer(tx=tx, block=block)

    url = 'v1/wallet/events?{query_params}'.format(
        query_params=urlencode({
            'blockchain_address': event.address,
            'blockchain_tip': tip.tip_hash,
        })
    )

    # when
    response = await cli.get(url)
    response_json = await response.json()
    response_json.pop('paging', None)

    data = {'events': [], 'pendingEvents': []} if case.has_empty_data_response else {
        'pendingEvents': [],
        'events': [
            {
                'events': [
                    {
                        'eventData': [
                            {'fieldName': key, 'fieldValue': value} for key, value in event.event_data.items()
                        ],
                        'eventIndex': event.event_index,
                        'eventType': event.type
                    }
                ],
                'transaction': {
                    'blockHash': tx.block_hash,
                    'blockNumber': tx.block_number,
                    'timestamp': tx.timestamp,
                    'from': getattr(tx, 'from'),
                    'gas': tx.gas,
                    'gasPrice': tx.gas_price,
                    'hash': tx.hash,
                    'input': tx.input,
                    'nonce': tx.nonce,
                    'status': True,
                    'r': tx.r,
                    's': tx.s,
                    'to': tx.to,
                    'transactionIndex': tx.transaction_index,
                    'v': tx.v,
                    'value': tx.value
                }
            }
        ]
    }

    assert response_json == {
        "status": API_STATUS_SUCCESS,
        "data": data,
        "meta": case.api_meta,
    }
