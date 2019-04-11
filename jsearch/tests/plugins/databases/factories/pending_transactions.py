import datetime

import factory
import pytest

from jsearch.common.tables import pending_transactions_t
from jsearch.tests.plugins.databases.factories.common import generate_address
from .common import session, Base


class PendingTransactionModel(Base):
    __table__ = pending_transactions_t


class PendingTransactionFactory(factory.alchemy.SQLAlchemyModelFactory):
    last_synced_id = factory.Faker('pyint')  # 0..9999
    hash = factory.LazyFunction(generate_address)
    status = ''
    timestamp = factory.LazyFunction(datetime.time)
    removed = False
    node_id = '1'
    r = '0xaa'
    s = '0xbb'
    v = '0xcc'
    to = factory.LazyFunction(generate_address)
    from_ = factory.LazyFunction(generate_address)
    gas = '0xabc'
    gas_price = '0x123'
    input = '0x00'
    nonce = factory.Sequence(lambda n: hex(n))
    value = factory.Sequence(lambda n: hex(n))

    class Meta:
        model = PendingTransactionModel
        sqlalchemy_session = session
        sqlalchemy_session_persistence = 'flush'
        rename = {'from_': 'from'}


@pytest.fixture()
def pending_transaction_factory():
    return PendingTransactionFactory
