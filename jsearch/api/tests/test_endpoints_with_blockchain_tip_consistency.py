import pytest

from jsearch.api.blockchain_tip import maybe_apply_tip
from pytest_mock import MockFixture
from typing import NamedTuple, Any, List, Callable
from urllib.parse import urlencode

from aiohttp.test_utils import TestClient

from jsearch.tests.plugins.databases.factories.accounts import AccountStateFactory, AccountFactory
from jsearch.tests.plugins.databases.factories.blocks import BlockFactory
from jsearch.tests.plugins.databases.factories.chain_events import ChainEventFactory
from jsearch.tests.plugins.databases.factories.internal_transactions import InternalTransactionFactory
from jsearch.tests.plugins.databases.factories.logs import LogFactory
from jsearch.tests.plugins.databases.factories.token_holder import TokenHolderFactory
from jsearch.tests.plugins.databases.factories.token_transfers import TokenTransferFactory
from jsearch.tests.plugins.databases.factories.transactions import TransactionFactory
from jsearch.tests.plugins.databases.factories.uncles import UncleFactory
from jsearch.tests.plugins.databases.factories.wallet_events import WalletEventsFactory


MaybeApplyTipPatcher = Callable[[str, List[int]], None]


@pytest.fixture
def _patch_maybe_apply_tip(mocker: MockFixture, chain_events_factory: ChainEventFactory) -> MaybeApplyTipPatcher:

    def wrapper(target_name: str, block_numbers_of_chain_splits: List[int]) -> None:
        async def maybe_apply_tip_and_split_the_chain_after_that(*args: Any, **kwargs: Any) -> Any:
            result = await maybe_apply_tip(*args, **kwargs)

            for block_number in block_numbers_of_chain_splits:
                chain_events_factory.create(block_number=block_number, type='split')

            return result

        mocker.patch(target_name, maybe_apply_tip_and_split_the_chain_after_that)

    return wrapper


class DataConsistencyCase(NamedTuple):
    block_number_of_data: int
    block_number_of_tip: int
    block_numbers_of_chain_splits: List[int]

    does_chain_split_affect_data_consistency: bool


# If chain split's block number is equal to 15, therefore data all the way up to
# block `15` is considered not forked:
#
#     --[15]---[16]-- Canonical branch
#           \
#            \
#             -[16]-- Forked branch


cases = [
    DataConsistencyCase(
        block_number_of_data=10,
        block_number_of_tip=15,
        block_numbers_of_chain_splits=[15],
        does_chain_split_affect_data_consistency=False,
    ),
    DataConsistencyCase(
        block_number_of_data=15,
        block_number_of_tip=10,
        block_numbers_of_chain_splits=[15],
        does_chain_split_affect_data_consistency=False,
    ),
    DataConsistencyCase(
        block_number_of_data=10,
        block_number_of_tip=15,
        block_numbers_of_chain_splits=[15, 14],
        does_chain_split_affect_data_consistency=True,
    ),
    DataConsistencyCase(
        block_number_of_data=15,
        block_number_of_tip=10,
        block_numbers_of_chain_splits=[15, 14],
        does_chain_split_affect_data_consistency=True,
    ),
    DataConsistencyCase(
        block_number_of_data=15,
        block_number_of_tip=10,
        block_numbers_of_chain_splits=[],
        does_chain_split_affect_data_consistency=False,
    ),
    DataConsistencyCase(
        block_number_of_data=15,
        block_number_of_tip=10,
        block_numbers_of_chain_splits=[],
        does_chain_split_affect_data_consistency=False,
    ),
]


@pytest.mark.parametrize('case', cases, ids=[repr(c) for c in cases])
async def test_get_accounts_balances_orphaned_requests(
        cli: TestClient,
        case: DataConsistencyCase,
        chain_events_factory: ChainEventFactory,
        block_factory: BlockFactory,
        account_state_factory: AccountStateFactory,
        _patch_maybe_apply_tip: MaybeApplyTipPatcher,
) -> None:
    # given
    block_of_tip = block_factory.create_with_event(chain_events_factory, number=case.block_number_of_tip)
    block_of_data = block_factory.create_with_event(chain_events_factory, number=case.block_number_of_data)

    account_state = account_state_factory.create(block_number=block_of_data.number)

    _patch_maybe_apply_tip('jsearch.api.handlers.accounts.maybe_apply_tip', case.block_numbers_of_chain_splits)

    url = 'v1/accounts/balances?{query_params}'.format(
        query_params=urlencode({
            'addresses': account_state.address,
            'blockchain_tip': block_of_tip.hash,
        })
    )

    # when
    response = await cli.get(url)
    response_json = await response.json()

    # then
    if case.does_chain_split_affect_data_consistency:
        assert response_json['data'] == {'isOrphaned': True}
    else:
        assert response_json['data'] != {'isOrphaned': True}


@pytest.mark.parametrize('case', cases, ids=[repr(c) for c in cases])
async def test_get_account_orphaned_requests(
        cli: TestClient,
        case: DataConsistencyCase,
        chain_events_factory: ChainEventFactory,
        block_factory: BlockFactory,
        account_factory: AccountFactory,
        account_state_factory: AccountStateFactory,
        _patch_maybe_apply_tip: MaybeApplyTipPatcher,
) -> None:
    # given
    block_of_tip = block_factory.create_with_event(chain_events_factory, number=case.block_number_of_tip)
    block_of_data = block_factory.create_with_event(chain_events_factory, number=case.block_number_of_data)

    account = account_factory.create()
    account_state_factory.create(block_number=block_of_data.number, address=account.address)

    _patch_maybe_apply_tip('jsearch.api.handlers.accounts.maybe_apply_tip', case.block_numbers_of_chain_splits)

    url = 'v1/accounts/{address}?{query_params}'.format(
        address=account.address,
        query_params=urlencode({
            'addresses': account.address,
            'blockchain_tip': block_of_tip.hash,
        })
    )

    # when
    response = await cli.get(url)
    response_json = await response.json()

    # then
    if case.does_chain_split_affect_data_consistency:
        assert response_json['data'] == {'isOrphaned': True}
    else:
        assert response_json['data'] != {'isOrphaned': True}


@pytest.mark.parametrize('case', cases, ids=[repr(c) for c in cases])
async def test_get_account_transactions_orphaned_requests(
        cli: TestClient,
        case: DataConsistencyCase,
        chain_events_factory: ChainEventFactory,
        block_factory: BlockFactory,
        transaction_factory: TransactionFactory,
        _patch_maybe_apply_tip: MaybeApplyTipPatcher,
) -> None:
    # given
    block_of_tip = block_factory.create_with_event(chain_events_factory, number=case.block_number_of_tip)
    block_of_data = block_factory.create_with_event(chain_events_factory, number=case.block_number_of_data)

    transaction, _ = transaction_factory.create_for_block(block_of_data)

    _patch_maybe_apply_tip('jsearch.api.handlers.accounts.maybe_apply_tip', case.block_numbers_of_chain_splits)

    url = 'v1/accounts/{txhash}/transactions?{query_params}'.format(
        txhash=transaction.hash,
        query_params=urlencode({
            'block_number': transaction.block_number,
            'limit': 1,
            'blockchain_tip': block_of_tip.hash,
        })
    )

    # when
    response = await cli.get(url)
    response_json = await response.json()

    # then
    if case.does_chain_split_affect_data_consistency:
        assert response_json['data'] == {'isOrphaned': True}
    else:
        assert response_json['data'] != {'isOrphaned': True}


@pytest.mark.parametrize('case', cases, ids=[repr(c) for c in cases])
async def test_get_account_internal_transactions_orphaned_requests(
        cli: TestClient,
        case: DataConsistencyCase,
        chain_events_factory: ChainEventFactory,
        block_factory: BlockFactory,
        internal_transaction_factory: InternalTransactionFactory,
        _patch_maybe_apply_tip: MaybeApplyTipPatcher,
) -> None:
    # given
    block_of_tip = block_factory.create_with_event(chain_events_factory, number=case.block_number_of_tip)
    block_of_data = block_factory.create_with_event(chain_events_factory, number=case.block_number_of_data)

    internal_tx = internal_transaction_factory.create(block_number=block_of_data.number)

    _patch_maybe_apply_tip('jsearch.api.handlers.accounts.maybe_apply_tip', case.block_numbers_of_chain_splits)

    url = 'v1/accounts/{address}/internal_transactions?{query_params}'.format(
        address=getattr(internal_tx, 'from'),
        query_params=urlencode({
            'block_number': internal_tx.block_number,
            'limit': 1,
            'blockchain_tip': block_of_tip.hash,
        })
    )

    # when
    response = await cli.get(url)
    response_json = await response.json()

    # then
    if case.does_chain_split_affect_data_consistency:
        assert response_json['data'] == {'isOrphaned': True}
    else:
        assert response_json['data'] != {'isOrphaned': True}


@pytest.mark.parametrize('case', cases, ids=[repr(c) for c in cases])
async def test_get_account_mined_blocks_orphaned_requests(
        cli: TestClient,
        case: DataConsistencyCase,
        chain_events_factory: ChainEventFactory,
        block_factory: BlockFactory,
        _patch_maybe_apply_tip: MaybeApplyTipPatcher,
) -> None:
    # given
    block_of_tip = block_factory.create_with_event(chain_events_factory, number=case.block_number_of_tip)
    block_of_data = block_factory.create_with_event(chain_events_factory, number=case.block_number_of_data)

    _patch_maybe_apply_tip('jsearch.api.handlers.accounts.maybe_apply_tip', case.block_numbers_of_chain_splits)

    url = 'v1/accounts/{address}/mined_blocks?{query_params}'.format(
        address=block_of_data.miner,
        query_params=urlencode({
            'block_number': block_of_data.number,
            'limit': 1,
            'blockchain_tip': block_of_tip.hash,
        })
    )

    # when
    response = await cli.get(url)
    response_json = await response.json()

    # then
    if case.does_chain_split_affect_data_consistency:
        assert response_json['data'] == {'isOrphaned': True}
    else:
        assert response_json['data'] != {'isOrphaned': True}


@pytest.mark.parametrize('case', cases, ids=[repr(c) for c in cases])
async def test_get_account_mined_uncles_orphaned_requests(
        cli: TestClient,
        case: DataConsistencyCase,
        chain_events_factory: ChainEventFactory,
        block_factory: BlockFactory,
        uncle_factory: UncleFactory,
        _patch_maybe_apply_tip: MaybeApplyTipPatcher,
) -> None:
    # given
    block_of_tip = block_factory.create_with_event(chain_events_factory, number=case.block_number_of_tip)
    block_of_data = block_factory.create_with_event(chain_events_factory, number=case.block_number_of_data)

    uncle = uncle_factory.create(block_number=block_of_data.number)

    _patch_maybe_apply_tip('jsearch.api.handlers.accounts.maybe_apply_tip', case.block_numbers_of_chain_splits)

    url = 'v1/accounts/{address}/mined_uncles?{query_params}'.format(
        address=uncle.miner,
        query_params=urlencode({
            'uncle_number': uncle.number,
            'limit': 1,
            'blockchain_tip': block_of_tip.hash,
        })
    )

    # when
    response = await cli.get(url)
    response_json = await response.json()

    # then
    if case.does_chain_split_affect_data_consistency:
        assert response_json['data'] == {'isOrphaned': True}
    else:
        assert response_json['data'] != {'isOrphaned': True}


@pytest.mark.parametrize('case', cases, ids=[repr(c) for c in cases])
async def test_get_account_token_transfers_orphaned_requests(
        cli: TestClient,
        case: DataConsistencyCase,
        chain_events_factory: ChainEventFactory,
        block_factory: BlockFactory,
        transfer_factory: TokenTransferFactory,
        transaction_factory: TransactionFactory,
        log_factory: LogFactory,
        _patch_maybe_apply_tip: MaybeApplyTipPatcher,
) -> None:
    # given
    block_of_tip = block_factory.create_with_event(chain_events_factory, number=case.block_number_of_tip)
    block_of_data = block_factory.create_with_event(chain_events_factory, number=case.block_number_of_data)

    block = block_factory.create(number=block_of_data.number)
    tx, _ = transaction_factory.create_for_block(block_of_data)
    log = log_factory.create_for_tx(tx)
    transfer, _ = transfer_factory.create_for_log(block, tx, log)

    _patch_maybe_apply_tip('jsearch.api.handlers.accounts.maybe_apply_tip', case.block_numbers_of_chain_splits)

    url = 'v1/accounts/{address}/token_transfers?{query_params}'.format(
        address=transfer.address,
        query_params=urlencode({
            'block_number': block_of_data.number,
            'blockchain_tip': block_of_tip.hash,
            'limit': 1,
        })
    )

    # when
    response = await cli.get(url)
    response_json = await response.json()

    # then
    if case.does_chain_split_affect_data_consistency:
        assert response_json['data'] == {'isOrphaned': True}
    else:
        assert response_json['data'] != {'isOrphaned': True}


@pytest.mark.parametrize('case', cases, ids=[repr(c) for c in cases])
async def test_get_account_token_balance_orphaned_requests(
        cli: TestClient,
        case: DataConsistencyCase,
        chain_events_factory: ChainEventFactory,
        block_factory: BlockFactory,
        token_holder_factory: TokenHolderFactory,
        _patch_maybe_apply_tip: MaybeApplyTipPatcher,
) -> None:
    # given
    block_of_tip = block_factory.create_with_event(chain_events_factory, number=case.block_number_of_tip)
    block_of_data = block_factory.create_with_event(chain_events_factory, number=case.block_number_of_data)

    token_holder = token_holder_factory.create(block_number=block_of_data.number)

    address = token_holder.account_address
    token_address = token_holder.token_address

    _patch_maybe_apply_tip('jsearch.api.handlers.accounts.maybe_apply_tip', case.block_numbers_of_chain_splits)

    url = 'v1/accounts/{address}/token_balance/{token_address}?{query_params}'.format(
        address=address,
        token_address=token_address,
        query_params=urlencode({
            'block_number': token_holder.block_number,
            'limit': 1,
            'blockchain_tip': block_of_tip.hash,
        })
    )

    # when
    response = await cli.get(url)
    response_json = await response.json()

    # then
    if case.does_chain_split_affect_data_consistency:
        assert response_json['data'] == {'isOrphaned': True}
    else:
        assert response_json['data'] != {'isOrphaned': True}


@pytest.mark.parametrize('case', cases, ids=[repr(c) for c in cases])
async def test_get_account_logs_orphaned_requests(
        cli: TestClient,
        case: DataConsistencyCase,
        chain_events_factory: ChainEventFactory,
        block_factory: BlockFactory,
        log_factory: LogFactory,
        _patch_maybe_apply_tip: MaybeApplyTipPatcher,
) -> None:
    # given
    block_of_tip = block_factory.create_with_event(chain_events_factory, number=case.block_number_of_tip)
    block_of_data = block_factory.create_with_event(chain_events_factory, number=case.block_number_of_data)

    log = log_factory.create(block_number=block_of_data.number)

    _patch_maybe_apply_tip('jsearch.api.handlers.accounts.maybe_apply_tip', case.block_numbers_of_chain_splits)

    url = 'v1/accounts/{address}/logs?{query_params}'.format(
        address=log.address,
        query_params=urlencode({
            'block_number': log.block_number,
            'limit': 1,
            'blockchain_tip': block_of_tip.hash,
        })
    )

    # when
    response = await cli.get(url)
    response_json = await response.json()

    # then
    if case.does_chain_split_affect_data_consistency:
        assert response_json['data'] == {'isOrphaned': True}
    else:
        assert response_json['data'] != {'isOrphaned': True}


@pytest.mark.parametrize('case', cases, ids=[repr(c) for c in cases])
async def test_get_blocks_orphaned_requests(
        cli: TestClient,
        case: DataConsistencyCase,
        chain_events_factory: ChainEventFactory,
        block_factory: BlockFactory,
        _patch_maybe_apply_tip: MaybeApplyTipPatcher,
) -> None:
    # given
    block_of_tip = block_factory.create_with_event(chain_events_factory, number=case.block_number_of_tip)
    block_of_data = block_factory.create_with_event(chain_events_factory, number=case.block_number_of_data)

    _patch_maybe_apply_tip('jsearch.api.handlers.blocks.maybe_apply_tip', case.block_numbers_of_chain_splits)

    url = 'v1/blocks?{query_params}'.format(
        query_params=urlencode({
            'block_number': block_of_data.number,
            'limit': 1,
            'blockchain_tip': block_of_tip.hash,
        })
    )

    # when
    response = await cli.get(url)
    response_json = await response.json()

    # then
    if case.does_chain_split_affect_data_consistency:
        assert response_json['data'] == {'isOrphaned': True}
    else:
        assert response_json['data'] != {'isOrphaned': True}


@pytest.mark.parametrize('case', cases, ids=[repr(c) for c in cases])
async def test_get_uncles_orphaned_requests(
        cli: TestClient,
        case: DataConsistencyCase,
        chain_events_factory: ChainEventFactory,
        block_factory: BlockFactory,
        uncle_factory: UncleFactory,
        _patch_maybe_apply_tip: MaybeApplyTipPatcher,
) -> None:
    # given
    block_of_tip = block_factory.create_with_event(chain_events_factory, number=case.block_number_of_tip)
    block_of_data = block_factory.create_with_event(chain_events_factory, number=case.block_number_of_data)

    uncle = uncle_factory.create(block_number=block_of_data.number)

    _patch_maybe_apply_tip('jsearch.api.handlers.uncles.maybe_apply_tip', case.block_numbers_of_chain_splits)

    url = 'v1/uncles?{query_params}'.format(
        query_params=urlencode({
            'uncle_number': uncle.block_number,
            'limit': 1,
            'blockchain_tip': block_of_tip.hash,
        })
    )

    # when
    response = await cli.get(url)
    response_json = await response.json()

    # then
    if case.does_chain_split_affect_data_consistency:
        assert response_json['data'] == {'isOrphaned': True}
    else:
        assert response_json['data'] != {'isOrphaned': True}


@pytest.mark.parametrize('case', cases, ids=[repr(c) for c in cases])
async def test_get_token_transfers_orphaned_requests(
        cli: TestClient,
        case: DataConsistencyCase,
        chain_events_factory: ChainEventFactory,
        block_factory: BlockFactory,
        transaction_factory: TransactionFactory,
        log_factory: LogFactory,
        transfer_factory: TokenTransferFactory,
        _patch_maybe_apply_tip: MaybeApplyTipPatcher,
) -> None:
    # given
    block_of_tip = block_factory.create_with_event(chain_events_factory, number=case.block_number_of_tip)
    block_of_data = block_factory.create_with_event(chain_events_factory, number=case.block_number_of_data)

    block = block_factory.create(number=block_of_data.number)
    tx, _ = transaction_factory.create_for_block(block)
    log = log_factory.create_for_tx(tx)
    transfer, _ = transfer_factory.create_for_log(block, tx, log)

    _patch_maybe_apply_tip('jsearch.api.handlers.tokens.maybe_apply_tip', case.block_numbers_of_chain_splits)

    url = 'v1/tokens/{address}/transfers?{query_params}'.format(
        address=transfer.token_address,
        query_params=urlencode({
            'block_number': transfer.block_number,
            'limit': 1,
            'blockchain_tip': block_of_tip.hash,
        })
    )

    # when
    response = await cli.get(url)
    response_json = await response.json()

    # then
    if case.does_chain_split_affect_data_consistency:
        assert response_json['data'] == {'isOrphaned': True}
    else:
        assert response_json['data'] != {'isOrphaned': True}


@pytest.mark.parametrize('case', cases, ids=[repr(c) for c in cases])
async def test_get_token_holders_orphaned_requests(
        cli: TestClient,
        case: DataConsistencyCase,
        chain_events_factory: ChainEventFactory,
        block_factory: BlockFactory,
        token_holder_factory: TokenHolderFactory,
        _patch_maybe_apply_tip: MaybeApplyTipPatcher,
) -> None:
    # given
    block_of_tip = block_factory.create_with_event(chain_events_factory, number=case.block_number_of_tip)
    block_of_data = block_factory.create_with_event(chain_events_factory, number=case.block_number_of_data)

    token_holder = token_holder_factory.create(block_number=block_of_data.number)

    _patch_maybe_apply_tip('jsearch.api.handlers.tokens.maybe_apply_tip', case.block_numbers_of_chain_splits)

    url = 'v1/tokens/{address}/holders?{query_params}'.format(
        address=token_holder.token_address,
        query_params=urlencode({
            'block_number': token_holder.block_number,
            'limit': 1,
            'blockchain_tip': block_of_tip.hash,
        })
    )

    # when
    response = await cli.get(url)
    response_json = await response.json()

    # then
    if case.does_chain_split_affect_data_consistency:
        assert response_json['data'] == {'isOrphaned': True}
    else:
        assert response_json['data'] != {'isOrphaned': True}


@pytest.mark.parametrize('case', cases, ids=[repr(c) for c in cases])
async def test_get_wallet_events_checks_data_consistency(
        cli: TestClient,
        case: DataConsistencyCase,
        chain_events_factory: ChainEventFactory,
        block_factory: BlockFactory,
        transaction_factory: TransactionFactory,
        wallet_events_factory: WalletEventsFactory,
        _patch_maybe_apply_tip: MaybeApplyTipPatcher,
) -> None:
    # given
    block_of_tip = block_factory.create_with_event(chain_events_factory, number=case.block_number_of_tip)
    block_of_data = block_factory.create_with_event(chain_events_factory, number=case.block_number_of_data)

    tx, _ = transaction_factory.create_for_block(block=block_of_data)
    event = wallet_events_factory.create_token_transfer(tx=tx, block=block_of_data)

    _patch_maybe_apply_tip('jsearch.api.handlers.wallets.maybe_apply_tip', case.block_numbers_of_chain_splits)

    url = 'v1/wallet/events?{query_params}'.format(
        query_params=urlencode({
            'blockchain_address': event.address,
            'blockchain_tip': block_of_tip.hash,
        })
    )

    # when
    response = await cli.get(url)
    response_json = await response.json()

    # then
    if case.does_chain_split_affect_data_consistency:
        assert response_json['data'] == {'isOrphaned': True}
    else:
        assert response_json['data'] != {'isOrphaned': True}


@pytest.mark.parametrize('event_type', ('split', 'reinserted', 'created'))
async def test_get_blocks_do_not_orphans_if_last_event_in_db_is_any_type(
        cli: TestClient,
        chain_events_factory: ChainEventFactory,
        block_factory: BlockFactory,
        event_type: str,
        _patch_maybe_apply_tip: MaybeApplyTipPatcher,
) -> None:
    """
    The latest event in the database is split
    """
    # given
    block_of_tip = block_factory.create_with_event(chain_events_factory, number=10)
    block_of_data = block_factory.create_with_event(chain_events_factory, number=10)

    # I.e., there can be an event, affecting clients' data *before* its
    # requests, but data should not be orphaned because it happened *before*
    # request and not in the middle of it.
    chain_events_factory.create(block_number=9, type=event_type)
    _patch_maybe_apply_tip('jsearch.api.handlers.wallets.maybe_apply_tip', [])

    url = 'v1/blocks?{query_params}'.format(
        query_params=urlencode({
            'block_number': block_of_data.number,
            'limit': 1,
            'blockchain_tip': block_of_tip.hash,
        })
    )

    # when
    response = await cli.get(url)
    response_json = await response.json()

    # then
    assert response_json['data'] != {'isOrphaned': True}
