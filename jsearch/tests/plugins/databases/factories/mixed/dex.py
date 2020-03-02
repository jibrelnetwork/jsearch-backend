from typing import Callable

import pytest

from jsearch.tests.plugins.databases.factories.blocks import BlockFactory
from jsearch.tests.plugins.databases.factories.dex_logs import create_dex_event_dict
from jsearch.tests.plugins.databases.factories.logs import LogFactory
from jsearch.tests.plugins.databases.factories.transactions import TransactionFactory
from jsearch.typing import AnyDict


@pytest.fixture
def tx_logs_with_dex_event_factory(
        block_factory: BlockFactory,
        block_dict_factory: Callable[..., AnyDict],
        transaction_factory: TransactionFactory,
        log_factory: LogFactory,
):
    def _factory(event_type: str, as_dict=True, block_kwargs=None, tx_kwargs=None, **kwargs) -> AnyDict:
        tx_kwargs = tx_kwargs or {}
        block_kwargs = block_kwargs or {}

        if as_dict:
            block = block_dict_factory(**block_kwargs)
        else:
            block = block_factory.create(as_dict=as_dict, **block_kwargs)

        tx = transaction_factory.create_for_block(block, as_dict=as_dict, **tx_kwargs)[0]
        kwargs = create_dex_event_dict(event_type, **kwargs)
        return log_factory.create_for_tx(event_type=event_type, as_dict=as_dict, tx=tx, **kwargs)

    return _factory
