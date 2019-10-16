from typing import Optional

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
        (4, 12, 40120000),
        (1, 323, 13230000),
        (999, 999, 9999990000),
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
        (4, 12, 123, 40125123),
        (1, 323, 0, 13235000),
        (999, 999, 9, 9999995009),
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
    "decimals_in_transfer, expected_decimals_in_event",
    (
        (12, '12'),
        (0, '0'),
        (None, '18'),
    )
)
def test_event_from_token_transfer_handles_decimals_correctly(
        transaction_factory: TransactionFactory,
        transfer_factory: TokenTransferFactory,
        decimals_in_transfer: Optional[int],
        expected_decimals_in_event: Optional[int],
) -> None:
    tx = factory.build(dict, FACTORY_CLASS=transaction_factory)
    transfer = factory.build(dict, FACTORY_CLASS=transfer_factory, token_decimals=decimals_in_transfer)

    event = wallet_events.event_from_token_transfer(address=tx["from"], tx_data=tx, transfer_data=transfer)
    decimals = event["event_data"]["decimals"]

    assert decimals == expected_decimals_in_event


@pytest.mark.parametrize(
    "block_number,transaction_index,internal_transaction_index,expected_event_index",
    (
        (4, 12, 123, 40120123),
        (1, 323, 0, 13230000),
        (999, 999, 9, 9999990009),
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


def test_different_events_have_same_magnitude(
        transaction_factory: TransactionFactory,
        transfer_factory: TokenTransferFactory,
        internal_transaction_factory: InternalTransactionFactory,
) -> None:
    tx = factory.build(dict, FACTORY_CLASS=transaction_factory, block_number=123, transaction_index=456, value='0x1')
    itx = factory.build(dict, FACTORY_CLASS=internal_transaction_factory, transaction_index=789, value=1)
    transfer = factory.build(dict, FACTORY_CLASS=transfer_factory, log_index=789)

    event_from__txs = wallet_events.event_from_tx(address=tx["from"], tx_data=tx)
    event_from_itxs = wallet_events.event_from_internal_tx(address=tx["from"], tx_data=tx, internal_tx_data=itx)
    event_from_logs = wallet_events.event_from_token_transfer(address=tx["from"], tx_data=tx, transfer_data=transfer)

    event_index_from__txs = event_from__txs["event_index"]
    event_index_from_itxs = event_from_itxs["event_index"]
    event_index_from_logs = event_from_logs["event_index"]

    assert event_index_from__txs == 1234560000
    assert event_index_from_itxs == 1234560789
    assert event_index_from_logs == 1234565789
