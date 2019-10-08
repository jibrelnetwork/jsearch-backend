import factory
import pytest

from jsearch.common.tables import receipts_t
from jsearch.tests.plugins.databases.factories.common import generate_address
from .common import session, Base


class ReceiptModel(Base):
    __table__ = receipts_t


class ReceiptFactory(factory.alchemy.SQLAlchemyModelFactory):
    transaction_hash = factory.LazyFunction(generate_address)
    block_number = factory.Sequence(lambda n: n)
    block_hash = factory.LazyFunction(generate_address)
    contract_address = factory.LazyFunction(generate_address)
    cumulative_gas_used = 1000000
    from_ = factory.LazyFunction(generate_address)
    to = factory.LazyFunction(generate_address)
    gas_used = 1000000
    logs_bloom = '0x'
    root = factory.LazyFunction(generate_address)
    transaction_index = factory.Sequence(lambda n: n)
    status = 0
    is_forked = False

    class Meta:
        model = ReceiptModel
        sqlalchemy_session = session
        sqlalchemy_session_persistence = 'flush'
        rename = {'from_': 'from'}

    @classmethod
    def create_for_tx(cls, tx, **kwargs):
        return cls.create(
            **{
                **kwargs,
                **{
                    'block_number': tx.block_number,
                    'block_hash': tx.block_hash,
                }
              })


@pytest.fixture()
def receipt_factory():
    return ReceiptFactory
