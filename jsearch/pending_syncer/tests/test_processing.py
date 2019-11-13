import pytest
from typing import Type

from jsearch.pending_syncer.structs import PendingTransaction
from jsearch.pending_syncer.utils.processing import prepare_pending_txs
from jsearch.tests.plugins.databases.factories.raw.pending_transactions import RawPendingTransactionFactory


def test_prepare_pending_txs_remaps_raw_txs_into_main_db_compliant_tuples(
        raw_pending_transaction_factory: Type[RawPendingTransactionFactory]
) -> None:
    raw_pending_tx = raw_pending_transaction_factory.create()
    processed_pending_txs = prepare_pending_txs([raw_pending_tx])

    assert processed_pending_txs == [
        PendingTransaction(
            last_synced_id=raw_pending_tx['id'],
            hash=raw_pending_tx['tx_hash'],
            timestamp=raw_pending_tx['timestamp'],
            removed=raw_pending_tx['removed'],
            node_id=raw_pending_tx['node_id'],
            status=raw_pending_tx['status'],
            r=raw_pending_tx['fields']['r'],
            s=raw_pending_tx['fields']['s'],
            v=raw_pending_tx['fields']['v'],
            to=raw_pending_tx['fields']['to'],
            from_=raw_pending_tx['fields']['from'],
            gas=raw_pending_tx['fields']['gas'],
            gas_price=raw_pending_tx['fields']['gasPrice'],
            input=raw_pending_tx['fields']['input'],
            nonce=raw_pending_tx['fields']['nonce'],
            value=raw_pending_tx['fields']['value'],
        )
    ]


@pytest.mark.freeze_time('2042-01-01')
def test_prepare_pending_txs_handles_incomplete_txs_data_without_complaints(
        raw_pending_transaction_factory: Type[RawPendingTransactionFactory]
) -> None:
    # WTF: These fields can be omitted by RawDB.
    raw_pending_tx = raw_pending_transaction_factory.create(
        status=None,
        fields={},
        node_id=None,
    )

    processed_pending_txs = prepare_pending_txs([raw_pending_tx])

    assert processed_pending_txs == [
        PendingTransaction(
            last_synced_id=raw_pending_tx['id'],
            hash=raw_pending_tx['tx_hash'],
            timestamp=raw_pending_tx['timestamp'],
            removed=raw_pending_tx['removed'],
            node_id=None,
            status=None,
            r=None,
            s=None,
            v=None,
            to=None,
            from_=None,
            gas=None,
            gas_price=None,
            input=None,
            nonce=None,
            value=None,
        )
    ]


@pytest.mark.parametrize('should_reverse', (True, False), ids=['reversed', 'forward'])
def test_prepare_pending_txs_overrides_data_if_there_are_multiple_txs_with_one_hash(
        raw_pending_transaction_factory: Type[RawPendingTransactionFactory],
        should_reverse: bool
) -> None:
    raw_pending_tx_before = raw_pending_transaction_factory.create(id=10, removed=False)
    raw_pending_tx_after = raw_pending_transaction_factory.create(
        id=20,
        tx_hash=raw_pending_tx_before['tx_hash'],
        removed=True,
        fields={},
    )

    raw_pending_txs = [raw_pending_tx_before, raw_pending_tx_after]

    if should_reverse:
        raw_pending_txs.reverse()

    processed_pending_txs = prepare_pending_txs(raw_pending_txs)

    assert processed_pending_txs == [
        PendingTransaction(
            # These fields will be changed.
            last_synced_id=raw_pending_tx_after['id'],
            timestamp=raw_pending_tx_after['timestamp'],
            removed=raw_pending_tx_after['removed'],
            node_id=raw_pending_tx_after['node_id'],
            status=raw_pending_tx_after['status'],

            # These fields won't be changed.
            hash=raw_pending_tx_before['tx_hash'],
            r=raw_pending_tx_before['fields']['r'],
            s=raw_pending_tx_before['fields']['s'],
            v=raw_pending_tx_before['fields']['v'],
            to=raw_pending_tx_before['fields']['to'],
            from_=raw_pending_tx_before['fields']['from'],
            gas=raw_pending_tx_before['fields']['gas'],
            gas_price=raw_pending_tx_before['fields']['gasPrice'],
            input=raw_pending_tx_before['fields']['input'],
            nonce=raw_pending_tx_before['fields']['nonce'],
            value=raw_pending_tx_before['fields']['value'],
        )
    ]
