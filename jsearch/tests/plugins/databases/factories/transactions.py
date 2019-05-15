import factory
import pytest

from jsearch.common.tables import transactions_t
from jsearch.tests.plugins.databases.factories.common import generate_address
from .common import session, Base


class TransactionModel(Base):
    __table__ = transactions_t
    __mapper_args__ = {
        'primary_key': [
            transactions_t.c.hash,
            transactions_t.c.block_hash,
            transactions_t.c.address,
        ]
    }

    def to_dict(self):
        return {k: getattr(self, k) for k in self.__table__.columns.keys()}


class TransactionFactory(factory.alchemy.SQLAlchemyModelFactory):
    from_ = factory.LazyFunction(generate_address)
    to = factory.LazyFunction(generate_address)
    address = factory.LazyFunction(generate_address)

    hash = factory.LazyFunction(generate_address)
    transaction_index = factory.Sequence(lambda n: n)

    block_number = factory.Sequence(lambda n: n)
    block_hash = factory.LazyFunction(generate_address)

    gas = '0xabc'
    gas_price = '0x123'
    input = '0x00'
    nonce = factory.Sequence(lambda n: n)
    r = '0xaa'
    s = '0xbb'
    v = '0xcc'
    value = factory.Sequence(lambda n: hex(n))
    contract_call_description = {}

    is_forked = False

    class Meta:
        model = TransactionModel
        sqlalchemy_session = session
        sqlalchemy_session_persistence = 'flush'
        rename = {'from_': 'from'}

    @classmethod
    def create_for_block(cls, block):
        data = cls.stub(block_number=block.number, block_hash=block.hash).__dict__
        data.pop('address', None)
        data['from_'] = data.pop('from', None)

        results = list()
        results.append(cls.create(address=data['from_'], **data))
        results.append(cls.create(address=data['to'], **data))

        return results


@pytest.fixture()
def transaction_factory():
    return TransactionFactory
