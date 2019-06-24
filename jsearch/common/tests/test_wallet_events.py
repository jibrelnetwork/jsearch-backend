import factory
import pytest

from jsearch.common import wallet_events
from jsearch.tests.plugins.databases.factories.internal_transactions import InternalTransactionFactory
from jsearch.tests.plugins.databases.factories.pending_transactions import PendingTransactionFactory
from jsearch.tests.plugins.databases.factories.token_transfers import TokenTransferFactory
from jsearch.tests.plugins.databases.factories.transactions import TransactionFactory


@pytest.mark.parametrize(
    "block_number,transaction_index,expected_event_index",
    (
        (4, 12, 4012000),
        (1, 323, 1323000),
        (999, 999, 999999000),
    )
)
def test_event_from_tx_event_index_is_formed_correctly(
        transaction_factory: TransactionFactory,
        block_number: int,
        transaction_index: int,
        expected_event_index: int,
) -> None:
    tx = factory.build(
        dict,
        FACTORY_CLASS=transaction_factory,
        block_number=block_number,
        transaction_index=transaction_index,
        value='0x1',  # i.e. WalletEventType.ETH_TRANSFER.
    )

    event = wallet_events.event_from_tx(address=tx["from"], tx_data=tx)
    event_index = event["event_index"]

    assert event_index == expected_event_index


@pytest.mark.parametrize(
    "block_number,transaction_index,log_index,expected_event_index",
    (
        (4, 12, 123, 4012123),
        (1, 323, 0, 1323000),
        (999, 999, 9, 999999009),
    )
)
def test_event_from_token_transfer_event_index_is_formed_correctly(
        transaction_factory: TransactionFactory,
        transfer_factory: TokenTransferFactory,
        block_number: int,
        transaction_index: int,
        log_index: int,
        expected_event_index: int,
) -> None:
    tx = factory.build(
        dict,
        FACTORY_CLASS=transaction_factory,
        block_number=block_number,
        transaction_index=transaction_index,
    )
    transfer = factory.build(dict, FACTORY_CLASS=transfer_factory, log_index=log_index)

    event = wallet_events.event_from_token_transfer(address=tx["from"], tx_data=tx, transfer_data=transfer)
    event_index = event["event_index"]

    assert event_index == expected_event_index


@pytest.mark.parametrize(
    "block_number,transaction_index,internal_transaction_index,expected_event_index",
    (
        (4, 12, 123, 4012123),
        (1, 323, 0, 1323000),
        (999, 999, 9, 999999009),
    )
)
def test_get_event_from_internal_tx_event_index_is_formed_correctly(
        transaction_factory: TransactionFactory,
        internal_transaction_factory: InternalTransactionFactory,
        block_number: int,
        transaction_index: int,
        internal_transaction_index: int,
        expected_event_index: int,
) -> None:
    tx = factory.build(
        dict,
        FACTORY_CLASS=transaction_factory,
        block_number=block_number,
        transaction_index=transaction_index,
    )
    internal_tx = factory.build(
        dict,
        FACTORY_CLASS=internal_transaction_factory,
        transaction_index=internal_transaction_index,
        value=1,
    )

    event = wallet_events.event_from_internal_tx(address=tx["from"], tx_data=tx, internal_tx_data=internal_tx)
    event_index = event["event_index"]

    assert event_index == expected_event_index


def test_get_event_from_pending_tx_event_index_is_formed_correctly(
        pending_transaction_factory: PendingTransactionFactory,
) -> None:
    tx = factory.build(dict, FACTORY_CLASS=pending_transaction_factory)
    event = wallet_events.get_event_from_pending_tx(address=tx["from"], pending_tx=tx)
    event_index = event["event_index"]

    assert event_index == 0
