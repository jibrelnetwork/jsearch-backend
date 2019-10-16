import pytest

from jsearch.api.blockchain_tip import maybe_apply_tip
from pytest_mock import MockFixture
from typing import NamedTuple, Any, List
from urllib.parse import urlencode

from aiohttp.test_utils import TestClient

from jsearch.tests.plugins.databases.factories.blocks import BlockFactory
from jsearch.tests.plugins.databases.factories.chain_events import ChainEventFactory
from jsearch.tests.plugins.databases.factories.transactions import TransactionFactory
from jsearch.tests.plugins.databases.factories.wallet_events import WalletEventsFactory

pytestmark = pytest.mark.usefixtures('disable_metrics_setup')


class DataConsistencyCase(NamedTuple):
    block_number_of_data: int
    block_number_of_tip: int
    block_numbers_of_chain_splits: List[int]

    does_chain_split_affect_data_consistency: bool


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
async def test_get_wallet_events_checks_data_consistency(
        cli: TestClient,
        mocker: MockFixture,
        case: DataConsistencyCase,
        chain_events_factory: ChainEventFactory,
        block_factory: BlockFactory,
        transaction_factory: TransactionFactory,
        wallet_events_factory: WalletEventsFactory,
) -> None:
    # given
    block_of_tip = block_factory.create(number=case.block_number_of_tip)
    block_of_data = block_factory.create(number=case.block_number_of_data)
    tx, _ = transaction_factory.create_for_block(block=block_of_data)
    event = wallet_events_factory.create_token_transfer(tx=tx, block=block_of_data)

    chain_events_factory.create_block(block=block_of_tip)
    chain_events_factory.create_block(block=block_of_data)

    async def maybe_apply_tip_and_split_the_chain_after_that(*args: Any, **kwargs: Any) -> Any:
        result = await maybe_apply_tip(*args, **kwargs)

        for block_number in case.block_numbers_of_chain_splits:
            chain_events_factory.create(block_number=block_number, type='split')

        return result

    mocker.patch('jsearch.api.handlers.wallets.maybe_apply_tip', maybe_apply_tip_and_split_the_chain_after_that)

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
