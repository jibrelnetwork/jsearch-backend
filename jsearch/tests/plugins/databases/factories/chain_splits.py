import factory
import pytest

from jsearch.common.tables import chain_splits_t
from .common import Base, generate_address, session


class ChainSplitModel(Base):
    __table__ = chain_splits_t


class ChainSplitFactory(factory.alchemy.SQLAlchemyModelFactory):
    id = factory.Sequence(lambda n: n)
    common_block_hash = factory.LazyFunction(generate_address)
    common_block_number = factory.Sequence(lambda n: n)
    drop_length = factory.Sequence(lambda n: n % 6)
    drop_block_hash = factory.LazyFunction(generate_address)
    add_length = factory.Sequence(lambda n: n % 6)
    add_block_hash = factory.LazyFunction(generate_address)
    node_id = factory.Sequence(lambda n: n % 10)

    class Meta:
        model = ChainSplitModel
        sqlalchemy_session = session
        sqlalchemy_session_persistence = 'flush'


@pytest.fixture()
def chain_split_factory():
    return ChainSplitFactory
