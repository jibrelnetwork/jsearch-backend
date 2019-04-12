import factory
import pytest

from jsearch.common.tables import pending_transactions_t
from jsearch.tests.plugins.databases.factories.common import generate_address, generate_psql_timestamp
from .common import session, Base


class PendingTransactionModel(Base):
    __table__ = pending_transactions_t


class PendingTransactionFactory(factory.alchemy.SQLAlchemyModelFactory):
    last_synced_id = factory.Faker('pyint')  # 0..9999
    hash = factory.LazyFunction(generate_address)
    status = ''
    timestamp = factory.LazyFunction(generate_psql_timestamp)
    removed = False
    node_id = '1'
    r = '0xaa'
    s = '0xbb'
    v = '0xcc'
    to = factory.LazyFunction(generate_address)
    from_ = factory.LazyFunction(generate_address)
    gas = factory.Faker('pyint')
    gas_price = factory.Faker('pyint')
    input = '0x00'
    nonce = factory.Sequence(lambda n: n)
    value = factory.Sequence(lambda n: str(n))

    class Meta:
        model = PendingTransactionModel
        sqlalchemy_session = session
        sqlalchemy_session_persistence = 'flush'
        rename = {'from_': 'from'}


@pytest.fixture()
def pending_transaction_factory():
    return PendingTransactionFactory
