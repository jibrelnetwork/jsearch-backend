import factory
import pytest

from jsearch.common.tables import reorgs_t
from .common import Base, generate_address, session


class ReorgModel(Base):
    __table__ = reorgs_t


class ReorgFactory(factory.alchemy.SQLAlchemyModelFactory):
    id = factory.Sequence(lambda n: n)
    block_hash = factory.LazyFunction(generate_address)
    block_number = factory.Sequence(lambda n: n)
    reinserted = factory.Sequence(lambda n: n % 2)  # each 2 item has reinserted sign
    node_id = factory.Sequence(lambda n: n % 10)

    class Meta:
        model = ReorgModel
        sqlalchemy_session = session
        sqlalchemy_session_persistence = 'flush'


@pytest.fixture()
def reorg_factory():
    return ReorgFactory
