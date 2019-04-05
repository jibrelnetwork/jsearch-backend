import time

import factory
import pytest

from jsearch.common.tables import internal_transactions_t
from jsearch.tests.plugins.databases.factories.common import generate_address
from .common import session, Base


class InternalTransactionModel(Base):
    __table__ = internal_transactions_t
    __mapper_args__ = {
        'primary_key': [
            internal_transactions_t.c.block_hash,
            internal_transactions_t.c.parent_tx_hash,
            internal_transactions_t.c.transaction_index,
        ]
    }

    def to_dict(self):
        return {k: getattr(self, k) for k in self.__table__.columns.keys()}


class InternalTransactionFactory(factory.alchemy.SQLAlchemyModelFactory):
    block_number = factory.Sequence(lambda n: n)
    block_hash = factory.LazyFunction(generate_address)
    parent_tx_hash = factory.LazyFunction(generate_address)
    op = 'call'
    call_depth = 1
    from_ = factory.LazyFunction(generate_address)
    to = factory.LazyFunction(generate_address)
    value = 0
    gas_limit = 1000000
    payload = '0x'
    status = 'success'
    transaction_index = factory.Sequence(lambda n: n)

    timestamp = factory.LazyFunction(time.time)
    is_forked = False

    class Meta:
        model = InternalTransactionModel
        sqlalchemy_session = session
        sqlalchemy_session_persistence = 'flush'
        rename = {'from_': 'from'}


@pytest.fixture()
def internal_transaction_factory():
    return InternalTransactionFactory
