from random import randint

import factory
import pytest

from jsearch.common.tables import wallet_events_t
from jsearch.common.wallet_events import make_event_index_for_internal_tx, make_event_index_for_log, \
    WalletEventType
from jsearch.tests.plugins.databases.factories.common import generate_address
from .common import session, Base


class WalletEventsModel(Base):
    __table__ = wallet_events_t
    __mapper_args__ = {
        'primary_key': [
            wallet_events_t.c.address,
            wallet_events_t.c.tx_hash,
            wallet_events_t.c.block_hash,
            wallet_events_t.c.block_number,
            wallet_events_t.c.event_index
        ]
    }


class WalletEventsFactory(factory.alchemy.SQLAlchemyModelFactory):
    address = factory.LazyFunction(generate_address)
    block_number = factory.Sequence(lambda n: n)
    block_hash = factory.LazyFunction(generate_address)
    tx_hash = factory.LazyFunction(generate_address)
    event_index = factory.Sequence(lambda n: n % 10)
    event_data = factory.LazyFunction(dict)
    type = 'erc20-transfer'
    is_forked = False

    class Meta:
        model = WalletEventsModel
        sqlalchemy_session = session
        sqlalchemy_session_persistence = 'flush'

    @classmethod
    def create_token_transfer(cls, tx, block=None, **kwargs):
        to = generate_address()
        from_ = generate_address()
        amount = randint(0, 10 * 18)

        block_number = tx.block_number
        transaction_index = tx.transaction_index
        item_index = randint(0, 10)

        defaults = dict(
            address=from_,
            type='erc20-transfer',
            event_data={'sender': to, 'recipient': from_, 'amount': amount},
            event_index=make_event_index_for_log(block_number, transaction_index, item_index)
        )

        defaults.update(**kwargs)
        if tx:
            defaults.update(
                tx_hash=tx.hash,
                tx_data=tx.to_dict()
            )

        if block:
            defaults.update(
                block_hash=block.hash,
                block_number=block.number
            )

        return cls.create(**defaults)

    @classmethod
    def create_event_from_internal_tx(cls, internal_tx, tx, block, **kwargs):
        to = getattr(internal_tx, "from")
        from_ = internal_tx.tx_origin

        amount = randint(0, 10 * 18)

        block_number = internal_tx.block_number
        transaction_index = internal_tx.parent_tx_index
        item_index = internal_tx.transaction_index

        defaults = dict(
            address=from_,
            type=WalletEventType.ETH_TRANSFER,
            event_data={'sender': to, 'recipient': from_, 'amount': amount},
            event_index=make_event_index_for_internal_tx(block_number, transaction_index, item_index)
        )
        defaults.update(**kwargs)
        if tx:
            defaults.update(
                tx_hash=tx.hash,
                tx_data=tx.to_dict()
            )

        if block:
            defaults.update(
                block_hash=block.hash,
                block_number=block.number
            )

        return cls.create(**defaults)


@pytest.fixture()
def wallet_events_factory():
    yield WalletEventsFactory
    WalletEventsFactory.reset_sequence()
