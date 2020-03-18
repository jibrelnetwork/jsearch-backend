from functools import partial

import factory
import pytest

from jsearch.common.tables import logs_t
from jsearch.tests.plugins.databases.factories.common import generate_address
from .common import session, Base


class LogModel(Base):
    __table__ = logs_t
    __mapper_args__ = {
        'primary_key': [
            logs_t.c.block_hash,
            logs_t.c.log_index,
            logs_t.c.transaction_index,
        ]
    }

    def to_dict(self):
        return {k: getattr(self, k) for k in self.__table__.columns.keys()}


class LogFactory(factory.alchemy.SQLAlchemyModelFactory):
    block_number = factory.Sequence(lambda n: n)
    block_hash = factory.LazyFunction(generate_address)
    log_index = factory.Sequence(lambda n: n)
    address = factory.LazyFunction(generate_address)
    data = ''
    removed = False
    topics = []
    transaction_hash = factory.LazyFunction(generate_address)
    transaction_index = factory.Sequence(lambda n: n)
    event_type = ''
    event_args = ''
    token_amount = 42
    token_transfer_from = factory.LazyFunction(generate_address)
    token_transfer_to = factory.LazyFunction(generate_address)
    is_token_transfer = True
    is_processed = True
    is_transfer_processed = True
    is_forked = False

    class Meta:
        model = LogModel
        sqlalchemy_session = session
        sqlalchemy_session_persistence = 'flush'

    @classmethod
    def create_for_tx(cls, tx, as_dict=False, **kwargs):
        get_tx_attr = tx.get if isinstance(tx, dict) else partial(getattr, tx)
        kwargs = {
            **{
                'block_number': get_tx_attr('block_number'),
                'block_hash': get_tx_attr('block_hash'),
                'timestamp': get_tx_attr('timestamp'),
                'address': get_tx_attr('from'),
                'transaction_hash': get_tx_attr('hash'),
                'transaction_index': get_tx_attr('transaction_index')
            },
            **kwargs,
        }
        if as_dict:
            return factory.build(dict, FACTORY_CLASS=cls, **kwargs)
        return cls.create(**kwargs)

    @classmethod
    def create_for_receipt(cls, receipt, **kwargs):
        return cls.create(
            **{
                **{
                    'block_number': receipt.block_number,
                    'block_hash': receipt.block_hash,
                    'address': getattr(receipt, 'from'),
                    'transaction_hash': receipt.transaction_hash,
                    'transaction_index': receipt.transaction_index
                },
                **kwargs,
            }
        )


@pytest.fixture()
def log_factory():
    return LogFactory
